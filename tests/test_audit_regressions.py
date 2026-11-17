"""全面复查发现的问题：每个用例对应实际状态/输入边界，不评测真实模型质量。"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from robotops_examples.pipeline.observer import PipelineObserver
from robotops_examples.pipeline.runtime import PipelineRuntime

from robotops.agent.prompt_loader import load_investigation_prompt
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import ConflictError, Store


def manual_report():
    return {
        "status": "needs_human",
        "summary": "测试报告，证据不足。",
        "recommended_action": {"kind": "manual_review", "reason": "需要进一步观测"},
    }


def test_core_accepts_real_component_names_without_example_defaults(tmp_path):
    with TestClient(create_app(Config(data_dir=tmp_path))) as client:
        body = {"app_id": "navigation", "components": ["planner", "controller"]}
        assert client.post("/applications", json=body).status_code == 201
        assert client.get("/applications/navigation").json()["components"] == body["components"]
        assert client.post("/applications", json={"app_id": "unspecified"}).status_code == 422


def test_prompt_input_stays_pinned_until_application_restart(tmp_path):
    snapshot = load_investigation_prompt()
    templates = {"system": snapshot.system, "task": snapshot.task}
    root = tmp_path / "prompts"
    folder = root / "investigation/v1"
    folder.mkdir(parents=True)

    def write_version(system):
        current = {**templates, "system": system}
        for name, text in current.items():
            (folder / f"{name}.txt").write_text(text)
        manifest = {
            "name": "investigation",
            "version": "v1",
            "change_note": "回归测试模板。",
            "sha256": hashlib.sha256(
                json.dumps(current, ensure_ascii=False, sort_keys=True).encode()
            ).hexdigest(),
        }
        (folder / "manifest.json").write_text(json.dumps(manifest))

    write_version(templates["system"])

    def model(messages, tools):
        assert "changed-after-startup" not in messages[0]["content"]
        return {"message": {"role": "assistant", "content": json.dumps(manual_report())}}

    app = create_app(
        Config(data_dir=tmp_path / "data", prompts_dir=root, prompt_version="v1"), complete=model
    )
    with TestClient(app) as client:
        pinned = client.get("/health").json()["prompt"]
        write_version(templates["system"] + "\nchanged-after-startup")
        assert (
            client.post(
                "/applications",
                json={"app_id": "pipeline", "components": ["producer", "processor"]},
            ).status_code
            == 201
        )
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "test"}).json()
        run = client.post(f"/incidents/{incident['id']}/investigate").json()
        assert run["state"] == "COMPLETED"
        snapshot = client.get(f"/runs/{run['id']}/trace").json()[0]
        assert snapshot["sha256"] == pinned["sha256"]


def test_failed_initialization_does_not_leave_runtime_marked_active(tmp_path, monkeypatch):
    runtime = PipelineRuntime(tmp_path)
    from robotops_examples.pipeline import runtime as runtime_module

    def fail_write(*args, **kwargs):
        raise OSError("injected configuration write failure")

    monkeypatch.setattr(runtime_module, "write_json", fail_write)
    with pytest.raises(OSError):
        runtime.start()
    assert runtime.runtime is None
    assert not runtime.processes


def test_terminal_run_cannot_be_overwritten_or_change_a_new_investigation(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="test"))
    first = store.start_run(incident["id"], "test")
    store.finish_run(first, report=manual_report())
    second = store.start_run(incident["id"], "test")
    with pytest.raises(ConflictError):
        store.finish_run(first, error="late failure from the old run")
    assert store.run(first)["state"] == "COMPLETED"
    assert store.run(second)["state"] == "RUNNING"
    assert store.incident(incident["id"])["state"] == "INVESTIGATING"


def test_concurrent_finish_has_only_one_winner(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="test"))
    run = store.start_run(incident["id"], "test")

    def finish():
        try:
            store.finish_run(run, report=manual_report())
            return True
        except ConflictError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _: finish(), range(2))) == 1


def test_bad_jsonl_does_not_hide_good_records_or_crash_queries(tmp_path):
    runtime = PipelineRuntime(tmp_path)
    runtime.runtime = tmp_path / "session"
    runtime.runtime.mkdir()
    row = {"at": "2026-11-17T00:00:00+00:00", "message": "useful failure"}
    (runtime.runtime / "producer.jsonl").write_bytes(
        json.dumps(row).encode() + b'\ninvalid-json\n{"at":"bad-time"}\n{"unfinished":'
    )
    result = PipelineObserver(runtime).search_logs("", 30, None)
    assert result["rows"] == [row]
    assert result["malformed_lines"] == 2
    assert result["incomplete_tail"] is True


@pytest.mark.parametrize("name", ["ROBOTOPS_DATA_DIR", "ROBOTOPS_PROMPTS_DIR"])
def test_empty_environment_paths_are_rejected(tmp_path, monkeypatch, name):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ROBOTOPS_CONFIG", raising=False)
    monkeypatch.setenv(name, " ")
    with pytest.raises(ValueError):
        Config.load()


def test_invalid_endpoint_port_is_rejected_at_configuration_time():
    with pytest.raises(ValueError):
        Config(model_base_url="http://localhost:invalid-port")
