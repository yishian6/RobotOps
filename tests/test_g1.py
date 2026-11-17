"""G1 的可复现检查：检索适用范围、证据保真、请求协议及故障用量，不评测 LLM 智能。"""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from robotops_benchmarks.context import compare, protocol_valid

from robotops.agent.client import DeepSeekClient
from robotops.agent.context import ContextSnapshots, aggregate_logs, assemble, canonical
from robotops.agent.memory import MemoryStore, WorkingMemory
from robotops.agent.workflow import investigate
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.retrieval import CaseCreate, CaseLibrary
from robotops.storage import Store
from robotops.tools import ToolGateway


@pytest.fixture
def investigation(tmp_path):
    store = Store(tmp_path)
    contract = ApplicationContract(
        app_id="navigation",
        version="2",
        components=["planner", "sensor"],
        dependencies={"planner": ["sensor"]},
    )
    store.register(contract)
    incident = store.create_incident(IncidentCreate(app_id="navigation", symptom="输入断流"))
    return store, contract, incident


def case_body(**changes):
    """案例时间相对当前时刻，避免测试依赖系统日期或本地时区。"""
    return {
        "app_id": "navigation",
        "app_version": "2",
        "components": ["sensor"],
        "title": "输入断流排查",
        "symptoms": "输入断流 lidar no output",
        "root_cause": "测试夹具中的采样停发",
        "resolution": "验证当前输入和实例",
        "source_uri": "fixture:verified-runbook",
        "verified_at": datetime.now(timezone.utc) - timedelta(days=2),
        **changes,
    }


def test_cases_filter_before_limit_and_preserve_provenance(investigation):
    store, contract, incident = investigation
    library = CaseLibrary(store)
    # 大量错版本/不适用案例不能挤掉正确结果；草稿、禁用、过期、未来验证均不可检出。
    for version in range(8):
        library.add(CaseCreate(**case_body(app_version=f"wrong-{version}")))
    for changes in (
        {"app_id": "other"},
        {"components": ["unknown"]},
        {"verified_at": None},
        {"enabled": False},
        {"valid_until": datetime.now(timezone.utc) - timedelta(days=1)},
        {"verified_at": datetime.now(timezone.utc) + timedelta(days=1)},
    ):
        library.add(CaseCreate(**case_body(**changes)))
    valid = library.add(CaseCreate(**case_body()))
    found = library.search(contract.model_dump(), "输入断流", limit=1)
    assert [row["id"] for row in found] == [valid["id"]]
    assert library.get(valid["id"])["source_uri"] == "fixture:verified-runbook"
    assert library.search(contract.model_dump(), "no output")[0]["id"] == valid["id"]
    assert library.search(contract.model_dump(), '" OR * NOT ()') == []
    assert library.search(contract.model_dump(), "nothing_matches") == []
    assert library.search(contract.model_dump(), "断流", components=["planner"]) == []
    with pytest.raises(ValueError):
        library.search(contract.model_dump(), "断流", components=["unknown"])
    run_id = store.start_run(incident["id"], "fixture")
    result = ToolGateway(store, None, run_id).execute("retrieve_cases", {"query": "输入断流"})
    assert result["data"]["current_observation"] is False
    assert result["data"]["cases"][0]["id"] == valid["id"]
    upstream = ToolGateway(store, None, run_id).execute(
        "retrieve_cases", {"query": "输入断流", "components": ["planner"]}
    )
    assert upstream["data"]["cases"][0]["id"] == valid["id"]
    assert store.read_evidence(run_id, result["evidence_id"])["raw"]["data"] == result["data"]
    library.disable(valid["id"])
    assert library.search(contract.model_dump(), "断流") == []
    assert store.read_evidence(run_id, result["evidence_id"])["raw"]["data"]["cases"][0]["enabled"]


def test_memory_scope_and_monotonic_counter_evidence(investigation):
    store, _, incident = investigation
    run_id = store.start_run(incident["id"], "fixture")
    support = store.save_evidence(run_id, "query_metrics", {"status": "ok", "value": 0})
    counter = store.save_evidence(run_id, "query_metrics", {"status": "ok", "value": 20})
    memory = MemoryStore(store, run_id)
    result = memory.update(
        WorkingMemory(
            phase="investigation",
            focus_components=["planner"],
            hypotheses=[{"claim": "无输入", "support_ids": [support], "contradict_ids": [counter]}],
        )
    )
    assert set(result["protected_ids"]) == {support, counter}
    cleared = memory.update(WorkingMemory(phase="reporting"))
    assert cleared["version"] == 2 and set(cleared["protected_ids"]) == {support, counter}
    with pytest.raises(ValueError, match="outside"):
        memory.update(WorkingMemory(critical_ids=["ev-" + "f" * 32]))
    with pytest.raises(ValueError, match="outside"):
        memory.update(WorkingMemory(focus_components=["other"]))
    assert memory.current() == cleared
    # 并发写入不能回退版本或覆盖另一请求新增的保护引用。
    with ThreadPoolExecutor(max_workers=2) as pool:
        updates = list(
            pool.map(
                lambda eid: memory.update(WorkingMemory(critical_ids=[eid])), [support, counter]
            )
        )
    assert {row["version"] for row in updates} == {3, 4}
    next_incident = store.create_incident(IncidentCreate(app_id="navigation", symptom="另一次异常"))
    other = store.start_run(next_incident["id"], "fixture")
    with pytest.raises(ValueError):
        MemoryStore(store, other).update(WorkingMemory(critical_ids=[counter]))


def tool_turn(messages, number, name, result):
    """构造合法的完整批次，支持同轮多个工具时的原样保留检查。"""
    call = {
        "id": f"call-{number}",
        "type": "function",
        "function": {"name": name, "arguments": "{}"},
    }
    messages.extend(
        [
            {"role": "assistant", "tool_calls": [call]},
            {"role": "tool", "tool_call_id": call["id"], "content": canonical(result)},
        ]
    )


def test_log_aggregation_preserves_parameters_instances_and_time():
    first = {
        "at": "2026-11-17T08:00:00+08:00",
        "component": "sensor",
        "instance_id": "a",
        "message": "timeout",
        "elapsed_ms": 5,
        "unit": "ms",
    }
    rows = [
        first,
        {**first, "at": "2026-11-17T00:00:01Z"},
        {**first, "elapsed_ms": 50},
        {**first, "instance_id": "b"},
        {**first, "component": "planner"},
    ]
    result = aggregate_logs({"rows": rows, "malformed_lines": 2, "incomplete_tail": True})
    assert result["malformed_lines"] == 2 and result["incomplete_tail"]
    groups = result["rows"]["groups"]
    assert len(groups) == 4 and groups[0]["count"] == 2
    assert groups[0]["first_at"] == first["at"]
    assert groups[0]["last_at"] == "2026-11-17T00:00:01Z"
    assert {group["record"]["elapsed_ms"] for group in groups} == {5, 50}
    assert aggregate_logs({"rows": [{"at": "bad-time", "message": "x"}]}) == {
        "rows": [{"at": "bad-time", "message": "x"}]
    }


def test_context_protects_raw_and_refetch_and_keeps_protocol(investigation):
    _, contract, _ = investigation
    messages = [
        {"role": "system", "content": "fixture"},
        {"role": "user", "content": canonical({"contract": contract.model_dump()})},
    ]
    eid = "ev-" + "1" * 32
    old = {
        "status": "ok",
        "evidence_id": eid,
        "data": {
            "runtime_id": "old",
            "rows": [
                {
                    "at": "2026-11-17T00:00:00Z",
                    "value": "CRITICAL_COUNTER",
                    "component": "sensor",
                    "instance_id": "sensor-old",
                }
            ]
            * 100,
        },
    }
    tool_turn(messages, 1, "search_logs", old)
    tool_turn(
        messages,
        2,
        "query_metrics",
        {
            "status": "ok",
            "data": {
                "runtime_id": "new",
                "rate": 0,
                "telemetry": {"component": "sensor", "instance_id": "sensor-new"},
            },
        },
    )
    memory = {
        "version": 1,
        "memory": WorkingMemory(phase="investigation", focus_components=["planner"]).model_dump(),
        "protected_ids": [eid],
    }
    for strategy in ("full", "rolling", "evidence"):
        outgoing, manifest = assemble(messages, memory, strategy)
        assert protocol_valid(outgoing)
        assert manifest["dependency_scope"] == ["planner", "sensor"]
        if strategy != "full" and manifest["selected_view"] != "full_fallback":
            archive = json.loads(outgoing[2]["content"])["archive"]
            assert archive[0]["result"] == old and archive[0]["view"] == "protected_raw"
            assert archive[0]["instance_status"] == "previous_runtime"
        elif manifest["selected_view"] == "full_fallback":
            assert outgoing == messages
            assert manifest["evidence"][0]["instance_status"] == "previous_runtime"
        assert outgoing[-2:] == messages[-2:]
        if strategy != "full":
            assert manifest["evidence"][0]["previous_component_instances"] == [
                {"component": "sensor", "instance_id": "sensor-old"}
            ]
    tool_turn(messages, 3, "read_evidence", {"id": eid, "raw": old, "sha256": "fixture"})
    outgoing, _ = assemble(messages, memory)
    assert outgoing[-2:] == messages[-2:]  # 回取原文不再立即压缩。
    assert protocol_valid(outgoing)
    assert messages[3]["content"] == canonical(old)  # 规范历史不被就地改写。


def test_context_folds_only_explicitly_memorized_or_duplicate_observations():
    messages = [{"role": "system", "content": "fixture"}, {"role": "user", "content": "fixture"}]
    first, second = "ev-" + "1" * 32, "ev-" + "2" * 32
    tool_turn(
        messages,
        1,
        "query_metrics",
        {"evidence_id": first, "observed_at": "t1", "data": {"rate": 7}},
    )
    tool_turn(
        messages,
        2,
        "query_metrics",
        {"evidence_id": second, "observed_at": "t2", "data": {"rate": 7}},
    )
    tool_turn(messages, 3, "query_metrics", {"evidence_id": "ev-" + "3" * 32, "data": {"rate": 9}})
    memory = {
        "version": 1,
        "protected_ids": [],
        "memory": WorkingMemory(
            phase="reporting", facts=[{"statement": "t1 的输入速率为 7", "evidence_ids": [first]}]
        ).model_dump(),
    }
    outgoing, manifest = assemble(messages, memory)
    assert [entry.get("candidate_view", entry["view"]) for entry in manifest["evidence"]] == [
        "memory_reference",
        "duplicate_reference",
    ]
    assert (
        manifest["selected_view"] == "full_fallback"
        or "t1 的输入速率为 7" in outgoing[2]["content"]
    )
    assert outgoing[-1] == messages[-1]


def test_snapshot_roundtrip_scope_and_integrity(investigation):
    store, _, incident = investigation
    run_id = store.start_run(incident["id"], "fixture")
    snapshots = ContextSnapshots(store, run_id)
    original = [{"role": "user", "content": "原始请求"}]
    sequence = snapshots.save(original, [], {}, {"strategy": "full"})
    original[0]["content"] = "外部改写"
    assert snapshots.get(sequence)["messages"][0]["content"] == "原始请求"
    assert len(snapshots.list()) == 1
    with pytest.raises(KeyError):
        snapshots.get(2)
    with store.connection() as db:
        db.execute("UPDATE context_snapshots SET body='{}' WHERE run_id=?", (run_id,))
    with pytest.raises(ValueError, match="integrity"):
        snapshots.get(sequence)


def test_workflow_uses_memory_and_context_beyond_framework_default(investigation):
    store, _, incident = investigation
    captured, evidence_id = [], None

    def model(messages, tools):
        nonlocal evidence_id
        captured.append(messages)
        turn = len(captured)
        result = json.loads(messages[-1]["content"]) if messages[-1]["role"] == "tool" else {}
        if turn == 2:
            evidence_id = result["evidence_id"]
            args = {
                "phase": "investigation",
                "facts": [{"statement": "契约需要 sensor 输入", "evidence_ids": [evidence_id]}],
                "critical_ids": [evidence_id],
            }
            name = "update_working_memory"
        elif turn < 17:
            name, args = "get_application_contract", {}
        else:
            return {
                "message": {"role": "assistant", "content": json.dumps(manual_report())},
                "usage": {"prompt_tokens": 3, "completion_tokens": 2},
            }
        return {
            "message": {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": f"call-{turn}",
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(args)},
                    }
                ],
            },
            "usage": {"prompt_tokens": 3, "completion_tokens": 2},
        }

    run = investigate(store, None, incident["id"], model, "fixture", context_strategy="evidence")
    assert run["state"] == "COMPLETED" and len(captured) == 17
    snapshots = ContextSnapshots(store, run["id"])
    assert len(snapshots.list()) == 17
    for index, outgoing in enumerate(captured, 1):
        assert snapshots.get(index)["messages"] == outgoing and protocol_valid(outgoing)
    assert snapshots.get(17)["memory"]["protected_ids"] == [evidence_id]
    assert any(
        e["view"] == "duplicate_reference" for e in snapshots.get(17)["manifest"]["evidence"]
    )


def manual_report():
    return {
        "status": "needs_human",
        "summary": "仅做协议测试",
        "hypotheses": [],
        "recommended_action": {"kind": "manual_review", "reason": "缺少当前实测"},
    }


@pytest.mark.parametrize("invalid_protocol", [False, True])
def test_api_cases_usage_snapshots_and_invalid_model_usage(tmp_path, invalid_protocol):
    def model(*_):
        return {
            "message": (
                {"role": "assistant", "content": "invalid report"}
                if not invalid_protocol
                else {
                    "role": "assistant",
                    "tool_calls": [{"bad": "protocol"}],
                    "reasoning_content": "private-provider-field",
                }
            ),
            "usage": {"prompt_tokens": 123, "completion_tokens": 9},
        }

    with TestClient(create_app(Config(data_dir=tmp_path), complete=model)) as client:
        assert client.get("/health").json()["stage"] == "G1"
        client.post(
            "/applications", json={"app_id": "navigation", "version": "2", "components": ["sensor"]}
        )
        body = CaseCreate(**case_body()).model_dump(mode="json")
        case = client.post("/cases", json=body).json()
        assert client.get(f"/cases/{case['id']}").json() == case
        assert (
            len(client.get("/applications/navigation/cases", params={"query": "输入断流"}).json())
            == 1
        )
        assert (
            client.get(
                "/applications/navigation/cases", params={"query": "x", "limit": 0}
            ).status_code
            == 422
        )
        incident = client.post(
            "/incidents", json={"app_id": "navigation", "symptom": "断流"}
        ).json()
        run = client.post(f"/incidents/{incident['id']}/investigate").json()
        assert run["state"] == "FAILED"
        usage = client.get(f"/runs/{run['id']}/usage").json()
        assert usage["reported_tokens"] == {"prompt_tokens": 123, "completion_tokens": 9}
        assert usage["context_requests"] == 1
        assert "private-provider-field" not in json.dumps(
            client.get(f"/runs/{run['id']}/trace").json()
        )
        assert len(client.get(f"/runs/{run['id']}/contexts").json()) == 1
        snapshot = client.get(f"/runs/{run['id']}/contexts/1").json()
        assert snapshot["manifest"]["strategy"] == "evidence"
        assert client.get(f"/runs/{run['id']}/memory").json()["version"] == 0
        assert client.get("/runs/other/contexts/1").status_code == 404
        assert not client.delete(f"/cases/{case['id']}").json()["enabled"]
        assert client.get("/applications/navigation/cases", params={"query": "断流"}).json() == []


def test_transport_error_keeps_reported_usage(investigation):
    store, _, incident = investigation
    client = DeepSeekClient(
        Config(api_key="test-placeholder"),
        httpx.MockTransport(
            lambda _: httpx.Response(200, json={"choices": [], "usage": {"prompt_tokens": 19}})
        ),
    )
    run = investigate(store, None, incident["id"], client.complete, "fixture")
    assert run["state"] == "FAILED"
    events = store.trace(run["id"])
    assert events[1]["type"] == "model_error" and events[1]["usage"] == {"prompt_tokens": 19}


def test_case_is_never_accepted_as_current_root_cause(investigation):
    store, _, incident = investigation
    calls = 0

    def model(messages, _):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {
                "message": {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "history",
                            "type": "function",
                            "function": {"name": "retrieve_cases", "arguments": '{"query":"断流"}'},
                        }
                    ],
                }
            }
        eid = json.loads(messages[-1]["content"])["evidence_id"]
        report = {
            **manual_report(),
            "status": "proposed",
            "hypotheses": [
                {"cause": "把历史案例当当前结论", "assessment": "supported", "evidence_ids": [eid]}
            ],
        }
        return {"message": {"role": "assistant", "content": json.dumps(report)}}

    run = investigate(store, None, incident["id"], model, "fixture")
    assert run["state"] == "FAILED" and "current observation" in run["error"]


def test_offline_comparison_reports_failures_and_negative_savings():
    result = compare()
    assert result["real_model_calls"] == 0 and not result["token_cost_evaluated"]
    rows = result["results"]
    evidence = [row for row in rows if row["strategy"] == "evidence"]
    assert len(evidence) == 6 and all(not row["failures"] for row in evidence)
    assert any(row["byte_reduction_ratio"] == 0 for row in evidence)
    assert all(row["byte_reduction_ratio"] >= 0 for row in evidence)
    assert any(row["byte_reduction_ratio"] < 0 for row in rows)
    assert any(row["byte_reduction_ratio"] > 0 for row in evidence)
    assert any(row["failures"] for row in rows if row["strategy"] == "rolling")


def test_dependency_cycles_and_context_config_boundaries():
    with pytest.raises(ValidationError):
        ApplicationContract(
            app_id="app", components=["one", "two"], dependencies={"one": ["two"], "two": ["one"]}
        )
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="app", components=["one"], dependencies={"one": ["other"]})
    with pytest.raises(ValueError):
        Config(context_strategy="unknown")
