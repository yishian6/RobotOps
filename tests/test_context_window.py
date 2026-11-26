import json

import httpx
import pytest

from robotops.agent.client import DeepSeekClient
from robotops.agent.context import ContextSnapshots, assemble, canonical, fit_context_window
from robotops.agent.memory import WorkingMemory
from robotops.agent.recovery import InvestigationRecovery
from robotops.agent.usage import summarize_usage
from robotops.agent.workflow import investigate
from robotops.config import Config, ContextWindow
from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import Store
from robotops.tools import ToolGateway


def submit(evidence_ids=()):
    return {
        "role": "assistant",
        "content": json.dumps({
            "status": "needs_human",
            "summary": "已保留采样证据，等待人工复核。",
            "recommended_action": {
                "kind": "manual_review", "reason": "复核日志", "evidence_ids": list(evidence_ids),
            },
        }),
    }


def test_sdk_long_investigation_keeps_tool_pairs_and_retrievable_evidence(tmp_path):
    class Logs:
        app_id = "camera"
        sample = 0

        def search_logs(self, keyword, limit, since):
            self.sample += 1
            count = 110 if self.sample == 2 else 6
            return {
                "sample": self.sample, "output_hz": 0, "truncated": False,
                "lines": [f"{keyword}-{index} " + "图像发布超时 " * 20 for index in range(count)],
            }

    config = Config(
        api_key="fixture", context_compact_after_tokens=8000,
        context_window_tokens=17000, model_output_tokens=2048,
    )
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    incident = store.create_incident(IncidentCreate(app_id="camera", symptom="输出停滞"))
    requests, references = [], []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        assert body["max_tokens"] == 2048
        assert body["tools"] == requests[0]["tools"]
        messages = body["messages"]
        assert messages[:2] == requests[0]["messages"][:2]
        pending = set()
        for message in messages:
            if message["role"] == "tool":
                pending.remove(message["tool_call_id"])
            else:
                assert not pending
                pending = {call["id"] for call in message.get("tool_calls") or []}
        assert not pending
        turn = len(requests)
        if turn > 1:
            result = json.loads(messages[-1]["content"])
            references.append(result["evidence_id"])
            if turn == 3:
                assert result["view"] == "window_reference"
                assert result["data"] == {"output_hz": 0, "sample": 2, "truncated": False}
        message = submit(references) if turn == 7 else {
            "role": "assistant",
            "content": "记录一次调查步骤。" * 60,
            "tool_calls": [{
                "id": f"log-{turn}", "type": "function", "function": {
                    "name": "search_logs", "arguments": json.dumps({"keyword": f"sample-{turn}"}),
                },
            }],
        }
        return httpx.Response(200, json={"choices": [{"message": message}]})

    client = DeepSeekClient(config, transport=httpx.MockTransport(handler))
    run = investigate(
        store, Logs(), incident["id"], client.complete, config.model,
        context_strategy="full", context_window=config.context_window,
    )
    assert run["state"] == "COMPLETED", run
    assert len(requests) == 7
    assert run["report"]["recommended_action"]["evidence_ids"] == references
    assert len(set(references)) == 6
    snapshots = ContextSnapshots(store, run["id"])
    windows = [snapshots.get(index)["manifest"]["context_window"] for index in range(1, 8)]
    assert windows[0]["stage"] == "unchanged"
    assert windows[2]["stage"] == "tool_references"
    assert windows[2]["stages"][0] == "assistant_notes"
    assert windows[2]["remaining_estimated_tokens"] > 2000
    assert windows[3]["remaining_estimated_tokens"] > 2000
    assert all(item["fits"] and item["after_estimated_tokens"] <= 14952 for item in windows)
    archive = next(
        json.loads(item["content"])["archive"] for item in requests[3]["messages"][2:]
        if item["role"] == "user"
    )
    assert {item["evidence_id"] for item in archive} == set(references[:2])
    assert len(store.read_evidence(run["id"], references[1])["raw"]["data"]["lines"]) == 110


@pytest.mark.parametrize("input_limit, expected_requests", [(6000, 1), (500, 0)])
def test_context_window_switches_to_report_only_or_finishes_locally(
    tmp_path, input_limit, expected_requests,
):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    incident = store.create_incident(IncidentCreate(app_id="camera", symptom="输出停止"))
    requests = []

    def complete(messages, tools):
        requests.append(messages)
        assert [tool["function"]["name"] for tool in tools] == ["submit_report"]
        return {"message": submit()}

    run = investigate(
        store, None, incident["id"], complete, "fixture",
        context_window=ContextWindow(input_limit, input_limit + 1000, 1000),
    )
    assert run["state"] == "COMPLETED", run
    assert run["report"]["status"] == "needs_human"
    assert len(requests) == expected_requests
    snapshots = ContextSnapshots(store, run["id"])
    assert len(snapshots.list()) == expected_requests
    if requests:
        assert snapshots.get(1)["manifest"]["report_only"] == "context_window"
    else:
        budget, = store.trace(run["id"])
        assert budget["report_only"] == "context_window"
        assert budget["context_window"]["fits"] is False


def test_context_window_uses_configuration_overrides(monkeypatch, tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "context_window_tokens = 32768\ncontext_compact_after_tokens = 12000\nmodel_output_tokens = 1024\n"
    )
    monkeypatch.setenv("ROBOTOPS_CONFIG", str(path))
    monkeypatch.setenv("ROBOTOPS_MODEL_OUTPUT_TOKENS", "2048")
    assert Config.load().context_window == ContextWindow(12000, 32768, 2048, "deepseek_flash_calibrated")
    with pytest.raises(ValueError, match="larger than its output limit"):
        Config(context_window_tokens=100, model_output_tokens=100)


def test_window_keeps_both_sides_of_a_hypothesis_and_latest_user_constraint():
    protected = ["ev-" + digit * 32 for digit in ("1", "2")]
    memory = {
        "version": 1,
        "protected_ids": protected,
        "memory": WorkingMemory(
            phase="verification",
            hypotheses=[{
                "claim": "相机无信号", "assessment": "unverified",
                "support_ids": [protected[0]], "contradict_ids": [protected[1]],
            }],
        ).model_dump(),
    }
    initial = [{"role": "system", "content": "保持调查只读"}, {"role": "user", "content": "查相机"}]
    latest_user = {"role": "user", "content": "仅检查本次启动的实例"}
    history = list(initial)
    for index, evidence_id in enumerate(protected):
        if index == 1:
            history.append(latest_user)
        history.extend([
            {"role": "assistant", "tool_calls": [{
                "id": f"sample-{index}", "type": "function",
                "function": {"name": "search_logs", "arguments": "{}"},
            }]},
            {"role": "tool", "tool_call_id": f"sample-{index}", "content": canonical({
                "status": "ok", "evidence_id": evidence_id,
                "data": {"rate": index, "lines": ["event " * 3000]},
            })},
        ])
    outgoing, manifest = assemble(history, memory)
    compacted, audit = fit_context_window(
        history, outgoing, [], memory, manifest, ContextWindow(3584, 4096, 512),
    )
    assert audit["fits"] and audit["stage"] == "tool_references"
    assert compacted[:3] == [*initial, latest_user]
    assert json.loads(compacted[3]["content"])["memory"] == memory
    assert compacted[-2] == history[-2]
    assert json.loads(compacted[-1]["content"])["evidence_id"] == protected[1]
    assert "event event" in history[-1]["content"]


@pytest.mark.parametrize("output_override", [None, 32768])
def test_input_threshold_is_independent_of_generation_limit(output_override):
    config = Config(model_output_tokens=output_override)
    window = config.context_window
    assert window.compact_after_tokens == 131072
    assert window.input_limit_tokens == 1048576 - (output_override or 8192)
    memory = {"version": 0, "protected_ids": [], "memory": WorkingMemory().model_dump()}
    for size, compaction_attempted in ((250000, False), (270000, True)):
        history = [{"role": "system", "content": "保持只读"}, {"role": "user", "content": "x" * size}]
        outgoing, manifest = assemble(history, memory)
        fitted, audit = fit_context_window(history, outgoing, [], memory, manifest, window)
        assert audit["fits"] is True and fitted == history
        assert (audit["observed_batch_estimated_tokens"] is not None) is compaction_attempted


def test_window_retains_diagnostic_facts_and_prioritizes_explicit_evidence_reads():
    history = [{"role": "system", "content": "保持只读"}, {"role": "user", "content": "查输出停滞"}]
    memory = {"version": 0, "protected_ids": [], "memory": WorkingMemory().model_dump()}
    inspect_id, metrics_id = ("ev-" + digit * 32 for digit in ("1", "2"))
    process = {"alive": True, "pid": 42, "state": "S"}
    telemetry = {
        "component": "processor", "instance_id": "second", "processed_count": 4,
        "processing_started_at": "2026-11-25T04:42:28+00:00", "in_flight_processing_age_ms": 450,
    }
    components = {"processor": {"process": process, "telemetry": telemetry}}
    rates = {"producer": {"sent_hz": 18}, "processor": {"processed_hz": 0}}
    detailed = "diagnostic details " * 800
    observations = {
        "inspect_process": {"components": components, "detail": detailed},
        "query_metrics": {"rates": rates, "current": {"components": components}, "detail": detailed},
    }

    def tool_batch(calls):
        history.append({"role": "assistant", "tool_calls": [
            {"id": call_id, "type": "function", "function": {"name": tool, "arguments": "{}"}}
            for call_id, tool, _ in calls
        ]})
        history.extend(
            {"role": "tool", "tool_call_id": call_id, "content": canonical(result)}
            for call_id, _, result in calls
        )

    tool_batch([
        ("inspect", "inspect_process", {
            "status": "ok", "evidence_id": inspect_id, "data": observations["inspect_process"],
        }),
        ("metrics", "query_metrics", {
            "status": "ok", "evidence_id": metrics_id, "data": observations["query_metrics"],
        }),
    ])
    outgoing, manifest = assemble(history, memory)
    window = ContextWindow(4096, 50000, 512)
    compacted, audit = fit_context_window(history, outgoing, [], memory, manifest, window)
    assert audit["stage"] == "tool_references" and audit["after_estimated_tokens"] < 4096
    inspect, metrics = [json.loads(item["content"]) for item in compacted[-2:]]
    assert inspect["data"]["components"]["processor"]["process"] == process
    assert metrics["data"]["rates"] == rates
    assert metrics["data"]["current"]["components"]["processor"]["telemetry"] == telemetry
    assert "detail" not in metrics["data"]

    original = {
        "status": "ok", "id": metrics_id, "tool": "query_metrics",
        "raw": {"status": "ok", "data": observations["query_metrics"]},
    }
    tool_batch([("read", "read_evidence", original)])
    outgoing, manifest = assemble(history, memory)
    compacted, audit = fit_context_window(history, outgoing, [], memory, manifest, window)
    assert audit["fits"] and audit["after_estimated_tokens"] > window.compact_after_tokens
    assert json.loads(compacted[-1]["content"]) == original
    archive = json.loads(compacted[2]["content"])["archive"]
    assert next(entry for entry in archive if entry["tool"] == "query_metrics")["data"]["rates"] == rates

    compacted, audit = fit_context_window(
        history, outgoing, [], memory, manifest, ContextWindow(4096, 4608, 512),
    )
    assert audit["fits"] and audit["stage"] == "refetch_references"
    reduced = json.loads(compacted[-1]["content"])
    assert reduced["evidence_id"] == metrics_id and reduced["data"]["rates"] == rates
    assert "note" in reduced


def test_evidence_pages_count_new_rows_without_a_second_read(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    incident = store.create_incident(IncidentCreate(app_id="camera", symptom="输出停滞"))
    run_id = store.start_run(incident["id"], "fixture")
    rows = [
        {"at": "2026-11-25T00:00:00Z", "component": "camera", "sample": index, "detail": "x" * 20000}
        for index in range(30)
    ]
    data = {"rows": rows, "truncated": False}

    class Logs:
        app_id = "camera"

        def search_logs(self, keyword, limit, since):
            return data

    gateway = ToolGateway(store, Logs(), run_id)
    preview = gateway.execute("search_logs", {"limit": 30})
    assert preview["view"] == "log_preview"
    assert preview["page"] == {
        "offset": 29, "total": 30, "returned": 1, "next_offset": None, "omitted_rows": [29],
    }
    assert preview["data"]["rows"] == [{"at": rows[-1]["at"], "component": "camera"}]
    evidence_id = preview["evidence_id"]
    args = {"evidence_id": evidence_id}
    first = gateway.execute("read_evidence", args)
    assert first["page"]["next_offset"] == 1 and first["page"]["omitted_rows"] == [0]
    assert first["raw"]["data"]["rows"] == [{"at": rows[0]["at"], "component": "camera"}]
    last = gateway.execute("read_evidence", {**args, "offset": 20, "fields": ["sample"]})
    assert last["page"]["next_offset"] is None
    assert [row["sample"] for row in last["raw"]["data"]["rows"]] == list(range(20, 30))
    assert store.read_evidence(run_id, evidence_id)["raw"]["data"] == data
    assert first["id"] == last["id"] == evidence_id and first["sha256"] == last["sha256"]
    assert gateway.execute("read_evidence", args, visible_evidence=set())["status"] == "error"
    def unexpected_read(_):
        pytest.fail("The progress check should use the observation loaded by the gateway")

    recovery = InvestigationRecovery(unexpected_read)
    assert not recovery.record_progress("search_logs", preview)  # The only row has no usable body.
    assert not recovery.record_progress("read_evidence", first)
    for offset in (0, 10, 20):
        page = gateway.execute("read_evidence", {**args, "offset": offset, "limit": 10, "fields": ["sample"]})
        assert recovery.record_progress("read_evidence", page)
        assert not recovery.record_progress("read_evidence", page)
        narrower = gateway.execute("read_evidence", {**args, "offset": offset, "limit": 5, "fields": ["sample"]})
        assert not recovery.record_progress("read_evidence", narrower)


def test_context_elides_only_rows_and_fields_already_in_the_request():
    memory = {"version": 0, "protected_ids": [], "memory": WorkingMemory().model_dump()}
    prefix = [{"role": "system", "content": "只读"}, {"role": "user", "content": "查相机"}]
    # A subsequent user constraint keeps these turns in native tool-call form.
    history = [*prefix, {"role": "user", "content": "对照早晚日志"}]
    evidence_id = "ev-" + "1" * 32
    record = {"telemetry": {"sample": 1, "message": "x" * 2000}}
    new_record = {**record, "config_version": 2}

    def append_page(tool, payload):
        key = f"call-{len(history)}"
        history.extend([
            {"role": "assistant", "tool_calls": [{
                "id": key, "type": "function", "function": {"name": tool, "arguments": "{}"},
            }]},
            {"role": "tool", "tool_call_id": key, "content": canonical(payload)},
        ])

    append_page("search_logs", {
        "evidence_id": evidence_id, "view": "log_preview", "data": {"rows": [record]},
        "page": {"offset": 1, "returned": 1, "total": 2, "next_offset": None},
    })
    append_page("read_evidence", {
        "id": evidence_id, "view": "evidence_page", "raw": {"data": {"rows": [{"sample": 0}, record]}},
        "page": {"offset": 0, "returned": 2, "total": 2, "next_offset": None},
    })
    append_page("read_evidence", {
        "id": evidence_id, "view": "evidence_page", "raw": {"data": {"rows": [new_record]}},
        "page": {"offset": 1, "returned": 1, "total": 2, "next_offset": None},
    })
    outgoing, manifest = assemble(history, memory)
    assert manifest["request_bytes"] < manifest["source_bytes"]
    earlier = json.loads(outgoing[6]["content"])
    assert earlier["raw"]["data"]["rows"] == [{"sample": 0}]
    assert earlier["page"]["row_indices"] == [0]
    assert earlier["page"]["duplicate_rows"] == 1
    assert outgoing[-1] == history[-1]  # The newly requested config_version remains visible.
    assert json.loads(history[6]["content"])["raw"]["data"]["rows"][-1] == record


def test_captured_console_does_not_replace_live_component_instances():
    memory = {"version": 0, "protected_ids": [], "memory": WorkingMemory().model_dump()}
    history = [{"role": "system", "content": "只读"}, {"role": "user", "content": "查相机"}]
    samples = [
        ("inspect_process", {"runtime_id": "live", "components": {"camera": {"telemetry": {
            "component": "camera", "instance_id": "live-camera",
        }}}}),
        ("search_logs", {"source": "captured_console", "runtime_id": "prior", "lines": ["old"]}),
        ("search_logs", {"source": "captured_console", "runtime_id": "prior", "lines": ["new"]}),
    ]
    for index, (tool, data) in enumerate(samples):
        key = f"runtime-{index}"
        history.extend([
            {"role": "assistant", "tool_calls": [{
                "id": key, "type": "function", "function": {"name": tool, "arguments": "{}"},
            }]},
            {"role": "tool", "tool_call_id": key, "content": canonical({"data": data})},
        ])
    _, manifest = assemble(history, memory, "rolling")
    assert [row["instance_status"] for row in manifest["evidence"]] == ["same_runtime", "previous_runtime"]


def test_window_drops_duplicate_replies_before_assistant_notes():
    memory = {"version": 0, "protected_ids": [], "memory": WorkingMemory().model_dump()}
    history = [{"role": "system", "content": "只读"}, {"role": "user", "content": "查相机"}]
    body = canonical({"evidence_id": "ev-" + "1" * 32, "data": {"line": "x" * 5000}})
    for index in range(3):
        key = f"tool-{index}"
        history += [
            {"role": "assistant", "content": "保留诊断计划", "tool_calls": [{
                "id": key, "type": "function",
                "function": {"name": "search_logs", "arguments": "{}"},
            }]},
            {"role": "tool", "tool_call_id": key, "content": body if index < 2 else "{}"},
        ]
    outgoing, manifest = assemble(history, memory, "full")
    fitted, audit = fit_context_window(
        history, outgoing, [], memory, manifest, ContextWindow(8000),
    )
    assert audit["stages"] == ["duplicate_history"]
    assert fitted[:2] == history[:2]
    assert fitted[4] == history[4] and fitted[6:] == history[6:]
    assert json.loads(fitted[5]["content"])["view"] == "duplicate_reference"


def test_memory_view_bounds_model_input_and_preserves_both_sides():
    support, counter = ("ev-" + digit * 32 for digit in ("1", "2"))
    memory = WorkingMemory(
        phase="verification",
        hypotheses=[{"claim": "可疑组件", "support_ids": [support], "contradict_ids": [counter]}],
        facts=[{"statement": "x" * 2000, "evidence_ids": [support]} for _ in range(12)],
    ).model_dump()
    snapshot = {"version": 1, "protected_ids": [support, counter], "memory": memory}
    history = [
        {"role": "system", "content": "只读"}, {"role": "user", "content": "查相机"},
        {"role": "assistant", "content": "目前缺少可验证的现场结果。"},
    ]
    outgoing, _ = assemble(history, snapshot, "rolling")
    selected = json.loads(outgoing[2]["content"])["memory"]
    assert len(canonical(selected).encode()) <= 8192
    assert selected["memory"]["hypotheses"] == memory["hypotheses"]
    assert selected["omitted"]["facts"] == 12 - len(selected["memory"]["facts"])
    assert len(memory["facts"]) == 12


@pytest.mark.parametrize("limit, success", [(2, True), (1, False)])
def test_provider_context_rejection_retries_once_with_a_smaller_report_request(tmp_path, limit, success):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    incident = store.create_incident(IncidentCreate(app_id="camera", symptom="输出停滞"))
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            return httpx.Response(400, json={"error": {
                "code": "context_length_exceeded", "message": "Maximum context length exceeded",
            }})
        return httpx.Response(200, json={"choices": [{"message": submit()}], "usage": {
            "prompt_tokens": 300, "completion_tokens": 20, "prompt_cache_hit_tokens": 100,
        }})

    config = Config(api_key="fixture")
    client = DeepSeekClient(config, transport=httpx.MockTransport(handler))
    run = investigate(
        store, None, incident["id"], client.complete, config.model,
        context_window=config.context_window, max_model_calls=limit,
    )
    assert run["state"] == "COMPLETED" and run["report"]["status"] == "needs_human"
    events = store.trace(run["id"])
    snapshots = ContextSnapshots(store, run["id"])
    usage = summarize_usage(events, snapshots.list(include_roles=True))["context_window"]
    assert usage["provider_rejections"] == 1 and usage["provider_retries"] == int(success)
    assert len(requests) == limit
    if success:
        first, last = usage["requests"]
        assert last["after_estimated_tokens"] < first["after_estimated_tokens"]
        assert last["actual_prompt_tokens"] == 300 and last["prompt_cache_hit_tokens"] == 100
        assert requests[1]["messages"] == requests[0]["messages"]
        assert [item["function"]["name"] for item in requests[1]["tools"]] == ["submit_report"]
