"""Accept pushed observations over a local Unix socket and dispatch the corresponding read-only investigation."""

import fcntl
import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
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
            listener.bind(str(path))
            try:
                path.chmod(0o600)
                store.interrupt_ros_fact_runs(app_id)
                with ThreadPoolExecutor(max_workers=1) as worker:
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
