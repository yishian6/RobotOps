"""
    M1 同步 API 回归。用可控观测和模型响应测并行、路由、隔离及失败合并，真实模型另做对照。
"""

import json
from threading import Barrier, Event
from time import monotonic, sleep

import pytest
from fastapi.testclient import TestClient

from robotops.agent.context import ContextSnapshots
from robotops.agent.usage import summarize_usage
from robotops.agent.workflow import investigate
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import Store


class Observer:
    """
        只提供测试需要的应用与系统观测，让网关继续执行实际范围校验和证据保存。
    """

    app_id = "pipeline"

    def __init__(self, alive: bool):
        """
            固定处理进程的实测替身状态。

            Args:
                alive: 该次测试是否模拟正在运行的处理进程。

            Returns:
                None: 保存本用例的进程状态。
        """
        self.alive = alive

    def inspect_process(self):
        """
            返回当前进程实例和状态。

            Returns:
                dict: 处理组件的进程观测。
        """
        return {"components": {"processor": {
            "process": {"alive": self.alive, "pid": 101, "cpu_time_seconds": 0.5},
            "telemetry": {
                "instance_id": "fixture", "processing_age_ms": 3200, "processing_latency_ms": 0.01,
                "processed_count": 2, "received_count": 3, "bound_port": 9000,
            },
        }}}

    def query_metrics(self, sample_seconds):
        """
            返回用于验证输出停滞的采样结果。

            Args:
                sample_seconds: 工具网关校验后的采样时长。

            Returns:
                dict: 非零输入和零处理输出。
        """
        return {
            "sample_seconds": sample_seconds, "input_hz": 10, "output_hz": 0,
            "current": self.inspect_process(),
        }


def reply(*, call=None, content=None):
    """
        构造测试模型响应，带可核对的输入、输出与缓存用量。

        Args:
            call: 可选工具调用三元组（ID、名称、参数）。
            content: 最终报告或分支发现。

        Returns:
            dict: 可由现有模型协议处理的响应字典。
    """
    message = {"role": "assistant"}
    if call:
        call_id, name, arguments = call
        message["tool_calls"] = [
            {"id": call_id, "type": "function", "function": {
                "name": name, "arguments": json.dumps(arguments),
            }},
        ]
    else:
        message["content"] = json.dumps(content, ensure_ascii=False)
    return {"message": message, "usage": {
        "prompt_tokens": 8, "completion_tokens": 2,
        "prompt_cache_hit_tokens": 3, "prompt_cache_miss_tokens": 5,
    }}


def submit_report(references):
    """
        用已有证据提交测试报告，具体诊断质量不由替身模拟。

        Args:
            references: 测试中真实得到的证据 ID。

        Returns:
            dict: 最终报告工具调用。
    """
    return reply(call=("report", "submit_report", {
        "status": "needs_human", "summary": "保存已核对的观测",
        "recommended_action": {
            "kind": "manual_review", "reason": "复核未验证部分", "evidence_ids": references,
        },
    }))


def test_concurrent_branches_share_the_parent_budget(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="输出停滞"))

    def model(_messages, tools):
        names = {tool["function"]["name"] for tool in tools}
        if names == {"submit_report"}:
            return submit_report([])
        if "submit_finding" in names:
            assert names == {"submit_finding"}
            return reply(content={"status": "blocked", "summary": "待取证", "missing_observations": ["指标"]})
        return reply(call=("dispatch", "dispatch_investigation", {"assignments": [
            {"role": "application", "question": "读指标"},
            {"role": "system", "question": "查进程"},
        ]}))

    run = investigate(
        store, Observer(True), incident["id"], model, "fixture", mode="supervised", max_model_calls=3,
    )
    assert run["state"] == "COMPLETED"
    events = store.trace(run["id"])
    calls = [event["agent_role"] for event in events if event["type"] == "model"]
    assert calls[0] == calls[-1] == "supervisor" and len(calls) == 3
    assert calls[1] in {"application", "system"}
    assert any(e.get("blocked_reason") == "model_call_budget" for e in events)
    budget = summarize_usage(events, ContextSnapshots(store, run["id"]).list(include_roles=True))[
        "model_budget"
    ]
    assert budget == {
        "limit": 3, "claimed": 3, "remaining": 0, "denied_claims": 1,
        "report_only": "model_call_budget",
    }


def test_branch_deadline_releases_parent_and_keeps_missing_usage(tmp_path):
    """
        验证单分支阻塞时主调查可完成，迟到回复只用于用量审计。

        Args:
            tmp_path: 独立的调查数据目录。

        Returns:
            None: 同一任务只有一个 blocked 终态，且没有迟到的工具调用。
    """
    release = Event()
    started = Event()

    def complete(messages, _tools):
        """
            用事件模拟一轮不会在分支截止时间内返回的模型请求。

            Args:
                messages: 主 Agent 或系统分支的实际消息。
                _tools: 角色可用的工具。

            Returns:
                dict: 固定的工具调用，单分支在测试释放后才响应。
        """
        assignment = json.loads(messages[1]["content"]).get("assignment")
        if assignment:
            started.set()
            if not release.wait(6):
                raise AssertionError("Parent investigation did not return")
            return reply(call=("late_tool", "inspect_process", {}))
        last = messages[-1]
        if last["role"] != "tool":
            return reply(call=("delegate", "dispatch_investigation", {
                "assignments": [{"role": "system", "question": "读取进程资源快照"}],
            }))
        body = json.loads(last["content"])
        assert started.is_set()
        assert not release.is_set()
        assert body["findings"][0]["status"] == "blocked"
        assert body["findings"][0]["blocked_reason"] == "deadline_exceeded"
        return submit_report([])

    config = Config(data_dir=tmp_path, branch_timeout_seconds=0.8)
    with TestClient(create_app(config, complete=complete, observer=Observer(True))) as client:
        client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).raise_for_status()
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "调查资源情况"}).json()
        try:
            run = client.post(f"/incidents/{incident['id']}/investigate", params={"mode": "supervised"}).json()
            assert run["state"] == "COMPLETED", run
            base = f"/runs/{run['id']}"
            usage = client.get(f"{base}/usage").json()
            assert usage["by_role"]["supervisor"]["recorded_attempts"] == 2
            system = usage["by_role"]["system"]
            assert system["context_requests"] == system["unreported_attempts"] == 1
            assert system["reported_cache_tokens"]["prompt_cache_hit_tokens"] is None
        finally:
            release.set()
        poll_until = monotonic() + 2
        while monotonic() < poll_until:
            usage = client.get(f"{base}/usage").json()
            if usage["by_role"]["system"]["recorded_attempts"]:
                break
            sleep(0.01)
        assert usage["by_role"]["system"]["reported_tokens"]["prompt_tokens"] == 8
        trace = client.get(f"{base}/trace").json()
        assert [row["status"] for row in trace if row["type"] == "branch_finished"] == ["blocked"]
        assert not any(row["type"] == "tool" and row["agent_role"] == "system" for row in trace)


def test_default_single_keeps_raw_observations(tmp_path):
    """
        核对默认单 Agent 取证、结构化交卷、模式列表和原始证据。

        Args:
            tmp_path: 本次调查独立的数据目录。

        Returns:
            None: 模型直查后两轮交卷，原始证据仍能按引用回取。
    """
    def model(messages, tools):
        """
            在同一轮请求进程和指标，取得结果后直接交报告。

            Args:
                messages: 当轮模型输入。
                tools: 当前可调用工具。

            Returns:
                dict: 两项观测调用或最终报告。
        """
        names = {tool["function"]["name"] for tool in tools}
        assert {"inspect_process", "query_metrics", "submit_report"} <= names
        assert "dispatch_investigation" not in names
        if messages[-1]["role"] != "tool":
            # 报告 Schema 已由 submit_report 提供，主提示词无需再发送一次。
            assert '"title": "DiagnosticReport"' not in messages[0]["content"]
            response = reply(call=("process", "inspect_process", {}))
            response["message"]["tool_calls"] += reply(
                call=("metrics", "query_metrics", {})
            )["message"]["tool_calls"]
            return response
        process, metrics = [json.loads(message["content"]) for message in messages[-2:]]
        assert process["view"] == metrics["view"] == "selected_fields"
        sampled = metrics["data"]["current"]["components"]["processor"]
        assert sampled["process"] == {"alive": True, "pid": 101}
        assert sampled["telemetry"]["in_flight_processing_age_ms"] == 3200
        assert sampled["telemetry"]["last_completed_processing_latency_ms"] == 0.01
        assert "processing_latency_ms" not in sampled["telemetry"]
        return submit_report([process["evidence_id"], metrics["evidence_id"]])

    with TestClient(create_app(Config(data_dir=tmp_path), complete=model, observer=Observer(True))) as client:
        health = client.get("/health").json()
        assert health["default_investigation_mode"] == "single"
        assert health["investigation_modes"] == ["single", "supervised"]
        client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).raise_for_status()
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "检查处理输出"}).json()
        endpoint = f"/incidents/{incident['id']}/investigate"
        assert client.post(endpoint, params={"mode": "adaptive"}).status_code == 422
        run = client.post(endpoint).json()
        assert run["state"] == "COMPLETED", run
        trace = client.get(f"/runs/{run['id']}/trace").json()
        assert not any(row["type"] == "branch_started" for row in trace)
        calls = [row for row in trace if row["type"] == "model"]
        assert [row["agent_role"] for row in calls] == ["single", "single"]
        for evidence_id in run["report"]["recommended_action"]["evidence_ids"]:
            raw = client.get(f"/runs/{run['id']}/evidence/{evidence_id}").json()["raw"]["data"]
            component = raw.get("current", raw)["components"]["processor"]
            assert component["process"]["cpu_time_seconds"] == 0.5
            assert component["telemetry"]["bound_port"] == 9000
            assert component["telemetry"]["processing_age_ms"] == 3200
            assert component["telemetry"]["processing_latency_ms"] == 0.01


@pytest.mark.parametrize("roles, leaked_reference", [
    (("application", "system"), False), (("system",), False), (("application", "system"), True),
])
def test_supervised_api_roles_evidence_and_trace(tmp_path, roles, leaked_reference):
    """
        验证双分支确实并行、系统单分支路径、未共享引用的拒绝及主图继续合并。

        Args:
            tmp_path: 独立数据库、轨迹和证据目录。
            roles: 测试主模型需要分派的角色。
            leaked_reference: 是否让系统分支试读并伪引同批未共享的应用证据。

        Returns:
            None: 分支轨迹、引用隔离和用量均通过断言。
    """
    barrier = Barrier(2) if len(roles) == 2 else None
    application_ready = Event()
    application_reference = None

    def model(messages, tools):
        """
            驱动主 Agent 和领域 Agent 的独立对话。

            Args:
                messages: 当前角色实际收到的上下文。
                tools: 当轮声明的工具。

            Returns:
                dict: 下一条工具请求或最终发现和报告。
        """
        nonlocal application_reference
        assignment = json.loads(messages[1]["content"]).get("assignment")
        body = json.loads(messages[-1]["content"]) if messages[-1]["role"] == "tool" else None
        names = {tool["function"]["name"] for tool in tools}
        if assignment is None:
            assert "submit_report" in names and "inspect_process" not in names and "query_metrics" not in names
            if body is None:
                assignments = [{"role": role, "question": f"核对 {role} 的事实"} for role in roles]
                dispatch = {"assignments": assignments}
                if roles == ("system",):
                    dispatch["confirm_exit_of"] = "processor"
                return reply(call=("batch", "dispatch_investigation", dispatch))
            if roles == ("system",) and body["status"] == "ok":
                assert body["confirmed_exit"]["component"] == "processor"
                # 模拟主 Agent 在得到决定性退出证据后仍请求扩张，程序应跳过这个分派。
                return reply(call=("unneeded", "dispatch_investigation", {
                    "assignments": [{"role": "application", "question": "继续看应用日志"}],
                }))
            # 父模型只收到结构化发现，即使某个分支失败也仍能审阅另一个分支的依据。
            if body["status"] == "skipped":
                references = [body["confirmed_exit"]["evidence_id"]]
            else:
                findings = body["findings"]
                assert [item["agent_role"] for item in findings] == list(roles)
                assert not any("raw" in item for item in findings)
                references = [eid for item in findings for eid in item.get("support_ids", [])]
            return reply(call=("report", "submit_report", {
                "status": "needs_human", "summary": "根据已得到的观测保存报告",
                "recommended_action": {
                    "kind": "manual_review", "reason": "下一步人工复核", "evidence_ids": references,
                },
            }))

        role = assignment["role"]
        tool = "inspect_process" if role == "system" else "query_metrics"
        assert {tool, "submit_finding"} <= names and "dispatch_investigation" not in names
        assert ("query_metrics" in names) == (role == "application")
        assert ("inspect_process" in names) == (role == "system")
        if body is None:
            if barrier:
                # 顺序执行会在这里超时。两个子图可同时进入首轮时才放行。
                barrier.wait(timeout=5)
            assert len(messages) == 2 and assignment["evidence_ids"] == []
            return reply(call=("observe", tool, {}))
        if role == "application":
            if body["status"] == "error":
                assert "requires newly collected domain observation" in body["error"]
                return reply(call=("finding-fixed", "submit_finding", {
                    "status": "complete", "summary": "已核对当前输出", "support_ids": [application_reference],
                }))
            application_reference = body["evidence_id"]
            application_ready.set()
            if not leaked_reference:
                # 结构化提交被拒后，应能利用已有观测修正，而不是让整条子调查失败。
                return reply(call=("incomplete", "submit_finding", {
                    "status": "complete", "summary": "缺少实测引用", "support_ids": [],
                }))
        if role == "system" and leaked_reference:
            assert application_ready.wait(timeout=5)
            if "evidence_id" in body:
                return reply(call=("foreign", "read_evidence", {"evidence_id": application_reference}))
            assert body["status"] == "error" and "scope" in body["error"]
            return reply(content={
                "status": "complete", "summary": "故意引用另一个分支的原文",
                "support_ids": [application_reference],
            })
        finding = {"status": "complete", "summary": f"已检查 {role}", "support_ids": [body["evidence_id"]]}
        if roles == ("system",):
            # 缺少退出机制时保留 blocked，但已经观测到退出仍应足以关闭分派门。
            finding.update(status="blocked", missing_observations=["退出码"])
        return reply(call=("finding", "submit_finding", finding))

    with TestClient(create_app(Config(data_dir=tmp_path), complete=model, observer=Observer(len(roles) == 2))) as client:
        assert client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).status_code == 201
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "检查处理输出"}).json()
        run = client.post(f"/incidents/{incident['id']}/investigate", params={"mode": "supervised"}).json()
        assert run["state"] == "COMPLETED", run
        base = f"/runs/{run['id']}"
        trace = client.get(f"{base}/trace").json()
        finished = {row["agent_role"]: row for row in trace if row["type"] == "branch_finished"}
        assert set(finished) == set(roles)
        expected_status = "failed" if leaked_reference else "blocked" if roles == ("system",) else "complete"
        assert finished["system"]["status"] == expected_status
        if leaked_reference:
            assert "outside this agent's scope" in finished["system"]["error"]
        started = next(i for i, row in enumerate(trace) if row["type"] == "dispatch_started")
        joined = next(i for i, row in enumerate(trace) if row["type"] == "dispatch_finished")
        assert all(row["type"] != "branch_finished" for row in trace[joined + 1:])
        assert not any(
            row["type"] in {"model", "model_error"} and row.get("agent_role") == "supervisor"
            for row in trace[started + 1:joined]
        )
        references = run["report"]["recommended_action"]["evidence_ids"]
        assert len(references) == len(roles) - leaked_reference
        for evidence_id in references:
            assert client.get(f"{base}/evidence/{evidence_id}").json()["raw"]["status"] == "ok"
        usage = client.get(f"{base}/usage").json()
        assert usage["reported_cache_tokens"]["prompt_cache_hit_tokens"] == usage["recorded_attempts"] * 3
        for row in trace:
            if row["type"] == "model":
                snap = client.get(f"{base}/contexts/{row['context_sequence']}").json()
                assert snap["manifest"]["agent_role"] == row["agent_role"]
                assert snap["manifest"].get("task_id") == row.get("task_id")


@pytest.mark.parametrize(("tool", "final_response"), [
    ("update_working_memory", "report"),
    ("update_working_memory", "repair"),
    ("dispatch_investigation", "unavailable_tool"),
    ("update_working_memory", "invalid_report"),
])
def test_real_validation_failures_stop_optional_retry(tmp_path, tool, final_response):
    """
        离线验证缺反证引用和不完整委派能够按字段纠错并收尾。

        Args:
            tmp_path: 独立的调查数据目录。
            tool: 模型尝试调用的可选工具。
            final_response: 修正后继续、正常收尾或无法交付有效报告的情况。

        Returns:
            None: 字段路径明确，重复同类问题后只再请求一次报告。
    """
    mode = "supervised" if tool == "dispatch_investigation" else "single"
    references = []

    def rejected_arguments(attempt):
        """
            构造措辞或列表下标有变化的同类错误。

            Args:
                attempt: 当前错误出现的次数。

            Returns:
                dict: 可重现已观测到的真实参数缺陷。
        """
        if tool == "update_working_memory":
            hypotheses = [{"claim": f"检查 {attempt} 的工作假设", "assessment": "refuted"}]
            if attempt == 2:
                hypotheses.insert(0, {"claim": "仍缺证的另一项假设"})
            return {"hypotheses": hypotheses}
        return {"assignments": [{
            "role": "system", "evidence_ids": [],
        }]}

    def model(messages, tools):
        """
            在真实工作流中驱动初查、参数拒绝、修正及报告边界。

            Args:
                messages: 已装配的模型输入。
                tools: 当前允许调用的工具定义。

            Returns:
                dict: 预先确定的模型响应。
        """
        names = {item["function"]["name"] for item in tools}
        last = messages[-1]
        if last["role"] != "tool":
            initial_tool = "query_metrics" if mode == "single" else "get_application_contract"
            return reply(call=("initial", initial_tool, {}))
        result = json.loads(last["content"])
        if last["tool_call_id"] == "initial":
            if mode == "single":
                references.append(result["evidence_id"])
            return reply(call=("reject_1", tool, rejected_arguments(1)))
        if last["tool_call_id"] == "repair":
            assert result["status"] == "ok" and result["version"] == 1
            assert tool in names and "inspect_process" in names
            return submit_report(references)
        assert result["status"] == "error"
        issue, = result["validation_errors"]
        if tool == "update_working_memory":
            index = int(last["tool_call_id"][-1]) - 1
            assert issue["path"] == ["hypotheses", index, "contradict_ids"]
            assert "unverified" in issue["message"]
        else:
            assert issue["path"] == ["assignments", 0, "question"]
        if last["tool_call_id"] == "reject_1":
            assert tool in names
            if final_response == "repair":
                payload = {"hypotheses": [{"claim": "仍待验证", "assessment": "unverified"}]}
                return reply(call=("repair", tool, payload))
            return reply(call=("reject_2", tool, rejected_arguments(2)))
        assert "next_step" in result
        assert names == {"submit_report"}
        if final_response == "unavailable_tool":
            return reply(call=("ignored", tool, rejected_arguments(3)))
        if final_response == "invalid_report":
            return reply(call=("invalid", "submit_report", {"status": "proposed"}))
        return submit_report(references)

    config = Config(data_dir=tmp_path, prompt_version="v1" if mode == "single" else "v2")
    app = create_app(config, complete=model, observer=Observer(True))
    with TestClient(app) as client:
        client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).raise_for_status()
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "输出停滞"}).json()
        run = client.post(f"/incidents/{incident['id']}/investigate", params={"mode": mode}).json()
        assert run["state"] == "COMPLETED", run
        assert run["report"]["status"] == "needs_human"
        assert run["report"]["recommended_action"]["evidence_ids"] == references
        base = f"/runs/{run['id']}"
        trace = client.get(f"{base}/trace").json()
        assert len([row for row in trace if row["type"] == "model"]) == 4
        assert not any(row["type"] == "branch_started" for row in trace)
        for evidence_id in references:
            assert client.get(f"{base}/evidence/{evidence_id}").json()["raw"]["status"] == "ok"
        marked = [row["tool"] for row in trace if row["type"] == "report_only"]
        assert marked == ([] if final_response == "repair" else [tool])
        latest = client.get(f"{base}/contexts/4").json()
        assert latest["manifest"].get("report_only") == (tool if marked else None)
        if final_response != "repair":
            assert "工作假设" not in latest["messages"][2].get("content", "") or not marked
