"""Import observations over loopback and dispatch the corresponding read-only investigation."""

import fcntl
import json
import sys
from http.client import HTTPConnection, HTTPException
from time import sleep
from typing import Literal

from pydantic import Field, model_validator

from .agent.client import DeepSeekClient, ModelError
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


class FactBatch(FactSource):
    events: list[TopicFact] = Field(max_length=100)

    @model_validator(mode="after")
    def check_order(self):
        sequences = [event.sequence for event in self.events]
        if sequences != sorted(set(sequences)):
            raise ValueError("Fact sequence numbers must be unique and ascending")
        return self


def fetch_facts(store: Store, app_id: str, port: int) -> FactBatch:
    """Read a bounded page of facts from the local collector, using its persisted identity."""
    connection = HTTPConnection("127.0.0.1", port, timeout=2)

    def read(path):
        connection.request("GET", path)
        with connection.getresponse() as response:
            if response.status != 200:
                raise ValueError(f"Collector responded with HTTP {response.status}")
            body = response.read(65537)
        if len(body) > 65536:
            raise ValueError("Collector response exceeded 64 KiB")
        return body

    try:
        source = FactSource.model_validate_json(read("/v1/source"))
        if source.app_id != app_id:
            raise ValueError("Collector belongs to a different application")
        cursor = store.ros_fact_cursor(source.source_id)
        batch = FactBatch.model_validate_json(read(f"/v1/events/{cursor}"))
        if source != FactSource.model_validate(batch.model_dump(exclude={"events"})):
            raise ValueError("Collector identity changed while reading events")
        if batch.events and batch.events[0].sequence <= cursor:
            raise ValueError("Collector returned events outside the requested page")
        return batch
    finally:
        connection.close()


def dispatch_facts(
    store: Store, config: Config, batch: FactBatch, complete, *, max_model_calls: int = 6,
) -> list[dict]:
    """Persist facts as tasks, then run each pending task once with a bounded model budget."""
    scope = batch.model_dump(exclude={"events"})
    store.ingest_ros_facts(batch.app_id, [{**scope, **event.model_dump()} for event in batch.events])
    return _run_pending_facts(store, config, batch.app_id, complete, max_model_calls)


def _run_pending_facts(
    store: Store, config: Config, app_id: str, complete, max_model_calls: int,
) -> list[dict]:
    """Investigate persisted observations independently of the collector connection."""
    pending = store.pending_ros_facts(app_id)
    if not pending:
        return []
    prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
    results = []
    for incident_id in pending:
        calls = 0

        def bounded_complete(messages, tools):
            nonlocal calls
            if calls >= max_model_calls:
                raise ModelError("Automatic ROS investigation model-call budget exhausted")
            calls += 1
            return complete(messages, tools)

        try:
            result = investigate(
                store, None, incident_id, bounded_complete, config.model, prompt,
                config.context_strategy, automatic=True,
            )
        except ConflictError:
            # A running manual investigation or a concurrent auto worker owns this event.
            continue
        results.append(result)
    return results


def watch_facts(
    config: Config, app_id: str, port: int, *, once: bool, max_model_calls: int,
) -> int:
    """Run one worker for a registered app, recovering interrupted runs on restart."""
    store = Store(config.data_dir)
    store.contract(app_id)
    model = None

    def complete(messages, tools):
        nonlocal model
        if model is None:
            model = DeepSeekClient(config)
        return model.complete(messages, tools)

    with (store.root / f".ros-facts-{app_id}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store.interrupt_ros_fact_runs(app_id)
        while True:
            try:
                batch = fetch_facts(store, app_id, port)
            except (OSError, HTTPException, ValueError) as exc:
                print(f"ROS 1 observation unavailable: {exc}", file=sys.stderr)
                exit_status = 1
                runs = _run_pending_facts(store, config, app_id, complete, max_model_calls)
            else:
                exit_status = 0
                runs = dispatch_facts(
                    store, config, batch, complete, max_model_calls=max_model_calls,
                )
            for run in runs:
                print(json.dumps(run, ensure_ascii=False), flush=True)
            if once:
                return exit_status
            sleep(config.monitor_interval_seconds)
