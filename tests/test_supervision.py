"""
    M1 同步 API 回归。用可控观测和模型响应测并行、路由、隔离及失败合并，真实模型另做对照。
"""

import json
from threading import Barrier, Event

import pytest
from fastapi.testclient import TestClient

from robotops.api import create_app
from robotops.config import Config


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
                "instance_id": "fixture", "processing_age_ms": 3200,
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


@pytest.mark.parametrize("confirm_exit", [False, True])
def test_adaptive_default_keeps_basic_queries_in_one_agent(tmp_path, confirm_exit):
    """
        验证不带 mode 的 API 直接取证，已有退出证据时跳过重复委派。

        Args:
            tmp_path: 独立运行目录。
            confirm_exit: 是否尝试重复确认主 Agent 已查到的退出事实。

        Returns:
            None: 核对实际请求数、工具和端到端证据引用。
    """
    def model(messages, tools):
        """
            同轮提出基础检查，随后交卷或请求一次冗余委派以验证后端分支门槛。

            Args:
                messages: 当前主 Agent 发送上下文。
                tools: 当前已绑定的工具定义。

            Returns:
                dict: 本轮模型响应。
        """
        names = {tool["function"]["name"] for tool in tools}
        assert {"inspect_process", "query_metrics", "dispatch_investigation", "submit_report"} <= names
        if messages[-1]["role"] != "tool":
            # 报告 Schema 只通过 submit_report 下发，系统指令不再重复整份定义。
            assert '"title": "DiagnosticReport"' not in messages[0]["content"]
            result = reply(call=("process", "inspect_process", {}))
            result["message"]["tool_calls"] += reply(
                call=("metrics", "query_metrics", {})
            )["message"]["tool_calls"]
            return result
        body = json.loads(messages[-1]["content"])
        if body.get("status") == "skipped":
            assert body["confirmed_exit"]["component"] == "processor"
            return submit_report([body["confirmed_exit"]["evidence_id"]])
        process, metrics = [json.loads(message["content"]) for message in messages[-2:]]
        assert process["view"] == metrics["view"] == "selected_fields"
        assert process["data"]["components"]["processor"]["telemetry"] == {"instance_id": "fixture"}
        sampled = metrics["data"]["current"]["components"]["processor"]
        assert sampled["process"] == {"alive": not confirm_exit, "pid": 101}
        assert sampled["telemetry"]["processing_age_ms"] == 3200
        refs = [process["evidence_id"], metrics["evidence_id"]]
        if confirm_exit:
            return reply(call=("unneeded", "dispatch_investigation", {
                "assignments": [{
                    "role": "system", "question": "确认处理进程退出了吗",
                    "reason": "测试重复确认", "expected_observation": "进程状态", "evidence_ids": refs,
                    "probe": {"tool": "inspect_process"},
                }],
                "confirm_exit_of": "processor",
            }))
        return submit_report(refs)

    with TestClient(create_app(Config(data_dir=tmp_path), complete=model, observer=Observer(not confirm_exit))) as client:
        client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).raise_for_status()
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "检查处理输出"}).json()
        assert client.get("/health").json()["default_investigation_mode"] == "adaptive"
        run = client.post(f"/incidents/{incident['id']}/investigate").json()
        assert run["state"] == "COMPLETED", run
        trace = client.get(f"/runs/{run['id']}/trace").json()
        assert not any(row["type"] == "dispatch_started" for row in trace)
        calls = [row for row in trace if row["type"] == "model"]
        assert len(calls) == 2 + confirm_exit
        assert all(row["investigation_mode"] == "adaptive" for row in calls)
        assert len(calls[0]["message"]["tool_calls"]) == 2
        for evidence_id in run["report"]["recommended_action"]["evidence_ids"]:
            current = client.get(f"/runs/{run['id']}/evidence/{evidence_id}").json()
            assert current["raw"]["status"] == "ok"
            raw = current["raw"]["data"]
            component = raw.get("current", raw)["components"]["processor"]
            assert component["process"]["cpu_time_seconds"] == 0.5
            assert component["telemetry"]["bound_port"] == 9000


@pytest.mark.parametrize("roles, mixed_batch", [
    (("application",), False), (("application", "system"), False), (("system",), True),
])
def test_adaptive_dispatches_after_observing_and_shares_evidence(tmp_path, roles, mixed_batch):
    """
        验证初查前拒绝分派，初查后按问题启动一个或两个分支并复用原有证据。

        Args:
            tmp_path: 独立运行目录。
            roles: 该场景需要委派的领域。
            mixed_batch: 是否在重复的应用检查同批加入尚未执行的系统检查。

        Returns:
            None: 核对分支数量、并行、证据访问及分派理由审计。
    """
    barrier = Barrier(2) if len(roles) == 2 else None
    collected = []

    def model(messages, tools):
        """
            驱动主 Agent 初查和两个独立角色的实际工具流程。

            Args:
                messages: 当前角色发送上下文。
                tools: 当前角色已绑定的工具定义。

            Returns:
                dict: 工具调用或最终发现。
        """
        task = json.loads(messages[1]["content"]).get("assignment")
        body = json.loads(messages[-1]["content"]) if messages[-1]["role"] == "tool" else None
        names = {tool["function"]["name"] for tool in tools}
        if task is None:
            if body is None:
                return reply(call=("early", "dispatch_investigation", {"assignments": []}))
            if body["status"] == "error":
                assert "before delegation" in body["error"]
                return reply(call=("triage", "query_metrics", {}))
            if body["status"] == "ok" and "findings" not in body:
                # 同样的指标检查即使改写任务描述、显式填写默认值，也不该再次开分支。
                assignments = [{
                    "role": "application", "question": "重新核实生产与消费速率",
                    "reason": "同样的检查", "expected_observation": "原速率",
                    "evidence_ids": [body["evidence_id"]],
                    "probe": {"tool": "query_metrics", "arguments": {"sample_seconds": 0.3}},
                }]
                if mixed_batch:
                    assignments.append({
                        "role": "system", "question": "核对进程存活", "reason": "基础速率不足以判断",
                        "expected_observation": "当前进程信息", "evidence_ids": [body["evidence_id"]],
                        "probe": {"tool": "inspect_process"},
                    })
                return reply(call=("duplicate", "dispatch_investigation", {"assignments": assignments}))
            if "findings" not in body:
                assert body["status"] == "skipped"
                if collected:
                    return submit_report(collected)
                evidence_id = body["skipped"][0]["evidence_id"]
                assert evidence_id
                return reply(call=("delegate", "dispatch_investigation", {"assignments": [
                    {
                        "role": role, "question": f"核对 {role} 的未解现象", "reason": "基础速率不足以判断",
                        "expected_observation": f"补查 {role} 证据", "evidence_ids": [evidence_id],
                        "probe": {"tool": "query_metrics", "arguments": {"sample_seconds": 1}}
                        if role == "application" else {"tool": "inspect_process"},
                    } for role in roles
                ]}))
            collected.extend(ref for finding in body["findings"] for ref in finding["support_ids"])
            if mixed_batch:
                assert [item["role"] for item in body["skipped"]] == ["application"]
                assert body["skipped"][0]["evidence_id"]
            # 已委派过的检查换成新问题与新引用，仍是同一次读数请求。
            return reply(call=("rephrase", "dispatch_investigation", {"assignments": [{
                "role": roles[0], "question": "换个问题复查同一项观测",
                "reason": "复述刚派的观察", "expected_observation": "同一次检查的数据",
                "evidence_ids": collected,
                "probe": {"tool": "inspect_process"} if mixed_batch else {
                    "tool": "query_metrics", "arguments": {"sample_seconds": 1.0},
                },
            }]}))

        assert "dispatch_investigation" not in names
        role = task["role"]
        assert len(task["evidence_ids"]) == 1
        if body is None:
            if barrier:
                # 两位领域 Agent 首轮一起进入，才能通过同步点。
                barrier.wait(timeout=5)
            return reply(call=("shared", "read_evidence", {"evidence_id": task["evidence_ids"][0]}))
        if "raw" in body:
            assert body["status"] == "ok" and body["tool"] == "query_metrics"
            if role == "application":
                return reply(call=("old-only", "submit_finding", {
                    "status": "complete", "summary": "只有旧实测", "support_ids": [body["id"]],
                }))
            return reply(call=("process", "inspect_process", {}))
        if body["status"] == "error":
            assert "newly collected probe" in body["error"]
            return reply(call=("sample", "query_metrics", {"sample_seconds": 1}))
        return reply(call=("finding", "submit_finding", {
            "status": "complete", "summary": "补充已核验事实", "support_ids": [body["evidence_id"]],
        }))

    with TestClient(create_app(Config(data_dir=tmp_path), complete=model, observer=Observer(True))) as client:
        client.post("/applications", json={"app_id": "pipeline", "components": ["processor"]}).raise_for_status()
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "检查处理输出"}).json()
        run = client.post(f"/incidents/{incident['id']}/investigate").json()
        assert run["state"] == "COMPLETED", run
        trace = client.get(f"/runs/{run['id']}/trace").json()
        started = [row for row in trace if row["type"] == "branch_started"]
        assert {row["agent_role"] for row in started} == set(roles)
        assert all(row["assignment"]["reason"] == "基础速率不足以判断" for row in started)
        dispatches = [row for row in trace if row.get("tool") == "dispatch_investigation"]
        statuses = ["error", "ok", "skipped"] if mixed_batch else ["error", "skipped", "ok", "skipped"]
        assert [row["result"]["status"] for row in dispatches] == statuses
        assert {row["status"] for row in trace if row["type"] == "branch_finished"} == {"complete"}


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
                assert "requires successful domain observation" in body["error"]
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
