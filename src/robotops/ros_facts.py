"""Import ROS observations and dispatch read-only investigations."""

import errno
import fcntl
import json
import socket
import sqlite3
import stat
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .agent.client import DeepSeekClient
from .agent.prompt_loader import load_investigation_prompt
from .agent.workflow import investigate
from .config import Config
from .models import Identifier, Model
from .storage import ConflictError, Store


class FactSource(Model):
    source_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    app_id: Identifier
    topic: str = Field(pattern=r"^/[A-Za-z0-9_/]+$", max_length=256)


class TopicFact(Model):
    sequence: int = Field(gt=0, le=2**63 - 1, strict=True)
    kind: Literal["topic_silent"]
    observed_at_ms: int = Field(gt=0, le=253402300799000, strict=True)
    last_message_at_ms: int = Field(gt=0, le=253402300799000, strict=True)
    silent_ms: int = Field(gt=0, strict=True)
    threshold_ms: int = Field(gt=0, strict=True)

    @model_validator(mode="after")
    def check_silence(self):
        if self.silent_ms < self.threshold_ms:
            raise ValueError("Silence is below the configured threshold")
        return self


class RosFact(FactSource, TopicFact):
    """One compact observation per datagram."""


def replay_facts(store: Store, app_id: str, path: Path) -> int:
    """Import missing rows from the collector's database without writing to it."""
    if not path.exists():
        return 0
    # The collector can keep writing with WAL enabled. The durable task key also covers
    # facts seen both by the startup query and a datagram queued during startup.
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA application_id").fetchone()[0] != 0x524F5346:
            raise ValueError(f"Not a RobotOps ROS facts database: {path}")
        source = db.execute(
            "SELECT id AS source_id, app AS app_id, topic FROM source WHERE slot=1"
        ).fetchone()
        if source is None:
            return 0
        source = FactSource.model_validate(dict(source))
        if source.app_id != app_id:
            raise ValueError("ROS facts database belongs to a different application")
        db.execute(
            "ATTACH DATABASE ? AS platform",
            ((store.root / "runtime.db").as_uri() + "?mode=ro",),
        )
        missing = db.execute(
            "SELECT f.* FROM topic_silences f WHERE NOT EXISTS ("
            "SELECT 1 FROM platform.ros_fact_tasks t WHERE t.source_id=? AND t.sequence=f.sequence"
            ") ORDER BY f.sequence",
            (source.source_id,),
        )
        imported = 0
        for row in missing:
            fact = RosFact.model_validate({**source.model_dump(), **dict(row), "kind": "topic_silent"})
            imported += store.ingest_ros_fact(app_id, fact.model_dump()) is not None
        return imported


def bind_fact_socket(listener: socket.socket, path: Path) -> None:
    try:
        listener.bind(str(path))
    except OSError as exc:
        if exc.errno != errno.EADDRINUSE:
            raise
        existing = path.lstat()
        if not stat.S_ISSOCK(existing.st_mode):
            raise
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as probe:
            try:
                probe.connect(str(path))
            except ConnectionRefusedError:
                current = path.lstat()
                if (current.st_dev, current.st_ino) != (existing.st_dev, existing.st_ino):
                    raise exc
                path.unlink()
                listener.bind(str(path))
            else:
                raise exc


def investigate_fact(
    store: Store, config: Config, incident_id: str, complete, max_model_calls: int,
) -> dict | None:
    prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
    try:
        return investigate(
            store, None, incident_id, complete, config.model, prompt,
            config.context_strategy, automatic=True, max_model_calls=max_model_calls,
        )
    except ConflictError:
        # A manual investigation can already own the event.
        return None


def watch_facts(
    config: Config, app_id: str, socket_path: Path | None, *, once: bool, max_model_calls: int,
    facts_db: Path | None = None,
) -> int:
    """Receive live observations while a single background worker handles investigations."""
    store = Store(config.data_dir)
    store.contract(app_id)
    path = socket_path or store.root / f"{app_id}-ros-facts.sock"
    model = None

    def complete(messages, tools):
        nonlocal model
        if model is None:
            model = DeepSeekClient(config)
        return model.complete(messages, tools)

    def run_and_print(incident_id):
        try:
            result = investigate_fact(store, config, incident_id, complete, max_model_calls)
        except (OSError, ValueError) as exc:
            print(f"ROS 1 investigation unavailable: {exc}", file=sys.stderr)
            return
        if result is not None:
            print(json.dumps(result, ensure_ascii=False), flush=True)

    with (store.root / f".ros-facts-{app_id}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as listener:
            bind_fact_socket(listener, path)
            try:
                path.chmod(0o600)
                store.interrupt_ros_fact_runs(app_id)
                imported = replay_facts(store, app_id, facts_db or store.root / "ros-facts.db")
                queued = store.queued_ros_fact_incidents(app_id)
                if imported or queued:
                    print(
                        f"ROS 1 startup: imported={imported} pending={len(queued)}",
                        file=sys.stderr, flush=True,
                    )
                if once and queued:
                    run_and_print(queued[0])
                    return 0
                with ThreadPoolExecutor(max_workers=1) as worker:
                    for incident_id in queued:
                        worker.submit(run_and_print, incident_id)
                    while True:
                        payload = listener.recv(4097)
                        try:
                            if len(payload) > 4096:
                                raise ValueError("ROS fact exceeded 4 KiB")
                            fact = RosFact.model_validate_json(payload)
                            if fact.app_id != app_id:
                                raise ValueError("ROS fact belongs to a different application")
                        except ValueError as exc:
                            print(f"Invalid ROS 1 observation: {exc}", file=sys.stderr)
                            continue
                        incident_id = store.ingest_ros_fact(app_id, fact.model_dump())
                        if incident_id is None:
                            continue
                        if once:
                            run_and_print(incident_id)
                            return 0
                        worker.submit(run_and_print, incident_id)
            finally:
                path.unlink(missing_ok=True)
