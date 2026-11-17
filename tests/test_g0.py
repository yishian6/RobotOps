"""核心存储、工具和模型协议检查；真实场景集成另见 test_pipeline.py。"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from robotops.agent.client import DeepSeekClient, ModelError
from robotops.agent.prompt_loader import load_investigation_prompt
from robotops.agent.workflow import investigate
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, DiagnosticReport, IncidentCreate
from robotops.storage import ConflictError, Store
from robotops.tools import ToolGateway, tool_definitions


@pytest.fixture
def runtime(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="程序没有输出"))
    return store, None, incident


def report(evidence_ids=()):
    return {
        "status": "needs_human",
        "summary": "测试替身只验证编排与引用，不评测模型诊断能力。",
        "hypotheses": [],
        "recommended_action": {
            "kind": "manual_review",
            "reason": "需要更多观测",
            "evidence_ids": list(evidence_ids),
        },
        "unresolved_questions": ["当前信息不足以确认根因"],
    }


def assistant_call(name, arguments, call_id="call-1"):
    return {
        "message": {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments)},
                }
            ],
        },
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }


def test_contract_boundary():
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="../../private", components=["worker"])
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="pipeline", components=["worker"], shell="anything")
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="pipeline", components=["producer", "producer"])
    with pytest.raises(ValidationError):
        DiagnosticReport.model_validate({**report(), "status": "proposed"})


def test_evidence_scope_and_integrity(runtime):
    store, _, incident = runtime
    first = store.start_run(incident["id"], "test-double")
    evidence = store.save_evidence(first, "sample", {"value": 1, "unit": "ms"})
    store.finish_run(first, report=report([evidence]))
    second = store.start_run(incident["id"], "test-double")
    with pytest.raises(KeyError):
        store.read_evidence(second, evidence)
    path = store.root / "evidence" / f"{evidence}.json"
    path.write_text('{"value": 2}')
    with pytest.raises(ValueError, match="integrity"):
        store.read_evidence(first, evidence)


def test_one_active_run_is_enforced_atomically(runtime):
    store, _, incident = runtime

    def start():
        try:
            return store.start_run(incident["id"], "test-double")
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: start(), range(2)))
    assert sum(value is not None for value in results) == 1


def test_missing_data_is_unavailable_and_tools_are_read_only(runtime):
    store, demo, incident = runtime
    run_id = store.start_run(incident["id"], "no-model")
    tools = ToolGateway(store, demo, run_id)
    missing = tools.execute("query_metrics", {})
    assert missing["status"] == "unavailable"
    assert "data" not in missing
    assert store.read_evidence(run_id, missing["evidence_id"])["raw"]["status"] == "unavailable"
    assert tools.execute("restart_application", {})["status"] == "error"
    assert tools.execute("run_shell", {"command": "anything"})["status"] == "error"
    assert tools.execute("query_metrics", {"sample_seconds": -1})["status"] == "error"
    assert tools.execute("search_logs", {"path": "/etc/passwd"})["status"] == "error"
    assert demo is None


def test_langgraph_executes_real_tools_and_persists_trace(runtime):
    store, demo, incident = runtime

    def model_double(messages, tools):
        assert any(item["function"]["name"] == "get_application_contract" for item in tools)
        observed = [json.loads(item["content"]) for item in messages if item["role"] == "tool"]
        if not observed:
            return assistant_call("get_application_contract", {})
        if len(observed) == 1:
            return assistant_call(
                "read_evidence", {"evidence_id": observed[0]["evidence_id"]}, "call-2"
            )
        assert observed[0]["data"]["app_id"] == "pipeline"
        assert observed[1]["raw"]["data"]["app_id"] == "pipeline"
        return {
            "message": {
                "role": "assistant",
                "content": json.dumps(report([observed[0]["evidence_id"]])),
            }
        }

    run = investigate(
        store,
        demo,
        incident["id"],
        model_double,
        "test-double-not-a-real-model",
        prompt=load_investigation_prompt("v1"),
        context_strategy="full",
    )
    assert run["state"] == "COMPLETED"
    assert store.incident(incident["id"])["state"] == "NEEDS_HUMAN"
    trace = store.trace(run["id"])
    assert [item["type"] for item in trace] == ["prompt", "model", "tool", "model", "tool", "model"]
    snapshot = trace[0]
    assert snapshot["version"] == "investigation/v1"
    assert len(snapshot["sha256"]) == 64
    actual_hash = hashlib.sha256(
        json.dumps(snapshot["messages"], ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    assert snapshot["messages_sha256"] == actual_hash
    assert snapshot["messages"][0]["role"] == "system"
    assert snapshot["tools"] == tool_definitions()
    assert trace[1]["usage"]["prompt_tokens"] == 10


@pytest.mark.parametrize(
    "message",
    [
        {"role": "assistant", "content": json.dumps(report(["ev-" + "0" * 32]))},
        {"role": "assistant", "content": "not a JSON report"},
        {"role": "assistant", "tool_calls": [{"bad": "protocol"}]},
    ],
)
def test_invalid_model_output_never_closes_incident(runtime, message):
    store, demo, incident = runtime
    run = investigate(store, demo, incident["id"], lambda *_: {"message": message}, "test-double")
    assert run["state"] == "FAILED"
    assert run["report"] is None
    assert store.incident(incident["id"])["state"] == "NEEDS_HUMAN"


def test_model_cannot_execute_an_unregistered_action(runtime):
    store, demo, incident = runtime

    def model_double(messages, _):
        results = [item for item in messages if item["role"] == "tool"]
        if not results:
            return assistant_call("restart_application", {"target": "processor"})
        assert json.loads(results[-1]["content"])["status"] == "error"
        return {"message": {"role": "assistant", "content": json.dumps(report())}}

    run = investigate(store, demo, incident["id"], model_double, "test-double")
    assert run["state"] == "COMPLETED"
    assert demo is None
    assert not store.evidence_ids(run["id"])


def test_deepseek_protocol_and_secret_handling(tmp_path):
    secret = "test-only-not-a-real-key"
    settings = Config(data_dir=tmp_path, api_key=secret)

    def handler(request):
        body = json.loads(request.content)
        assert str(request.url) == "https://api.deepseek.com/chat/completions"
        assert request.headers["authorization"] == "Bearer " + secret
        assert body["model"] == "deepseek-flash"
        assert body["thinking"] == {"type": "disabled"}
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "{}",
                            "reasoning_content": "not persisted",
                        }
                    }
                ],
                "usage": {"prompt_tokens": 3},
            },
        )

    result = DeepSeekClient(settings, httpx.MockTransport(handler)).complete([], tool_definitions())
    assert "reasoning_content" not in result["message"]
    assert secret not in repr(settings)
    assert secret not in json.dumps(result)
    with pytest.raises(ModelError, match="DEEPSEEK_API_KEY"):
        DeepSeekClient(Config(data_dir=tmp_path))


def test_api_missing_key_does_not_create_a_running_investigation(tmp_path):
    with TestClient(create_app(Config(data_dir=tmp_path))) as client:
        assert client.get("/health").json()["model_configured"] is False
        assert (
            client.post(
                "/applications",
                json={"app_id": "pipeline", "components": ["producer", "processor"]},
            ).status_code
            == 201
        )
        assert (
            client.post(
                "/applications",
                json={"app_id": "pipeline", "components": ["producer", "processor"]},
            ).status_code
            == 409
        )
        assert (
            client.post("/incidents", json={"app_id": "missing", "symptom": "x"}).status_code == 404
        )
        incident = client.post(
            "/incidents", json={"app_id": "pipeline", "symptom": "无输出"}
        ).json()
        assert client.post(f"/incidents/{incident['id']}/investigate").status_code == 503
        assert client.get(f"/incidents/{incident['id']}").json()["state"] == "OPEN"
        assert client.get("/runs/missing/trace").status_code == 404
