"""
    核心存储、工具和模型协议检查；真实场景集成另见 test_pipeline.py。
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from robotops.agent.client import DeepSeekClient, ModelError
from robotops.agent.context import ContextSnapshots
from robotops.agent.prompt_loader import load_investigation_prompt
from robotops.agent.recovery import InvestigationRecovery
from robotops.agent.workflow import _validate_report, investigate
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, DiagnosticReport, IncidentCreate
from robotops.storage import ConflictError, Store
from robotops.tools import ToolGateway, tool_definitions


def test_report_rejects_first_sample_stall_when_cited_observations_show_prior_completion(runtime):
    store, _, incident = runtime
    run_id = store.start_run(incident["id"], "fixture")
    evidence_id = store.save_evidence(run_id, "query_metrics", {
        "status": "ok",
        "data": {"current": {"components": {"processor": {"telemetry": {
            "processed_count": 4, "last_output_at": "2026-11-25T05:45:35.658744Z",
            "processing_started_at": "2026-11-25T05:45:35.709744Z",
        }}}}},
    })
    report = {
        "status": "proposed", "summary": "首个输入即卡住。",
        "hypotheses": [{
            "cause": "一个在途样本长期未完成",
            "assessment": "supported", "evidence_ids": [evidence_id],
        }],
        "recommended_action": {
            "kind": "collect_more_evidence", "target": "processor",
            "reason": "获取阻塞位置", "evidence_ids": [evidence_id],
        },
    }
    options = {
        "store": store, "run_id": run_id, "components": ["processor"],
        "visible_evidence": None, "required_evidence_id": None,
    }
    with pytest.raises(ModelError, match="已完成 4 个样本"):
        _validate_report(json.dumps(report, ensure_ascii=False), **options)
    report["summary"] = "采样时有一个在途样本未完成。此前已完成 4 个样本。"
    assert _validate_report(json.dumps(report, ensure_ascii=False), **options)["status"] == "proposed"
    report["summary"] = "证据不能断言首个输入即卡住。"
    assert _validate_report(json.dumps(report, ensure_ascii=False), **options)["status"] == "proposed"


@pytest.fixture
def runtime(tmp_path):
    """
        构建不启动真实进程的 G0 隔离调查夹具。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            tuple[Store, None, dict]: 已注册应用的 Store、空适配器和 OPEN 事件。
    """
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="程序没有输出"))
    return store, None, incident


def report(evidence_ids=()):
    """
        构造最小升级报告，专用于状态和引用协议检查。

        Args:
            evidence_ids: 待放入建议字段的引用序列，可故意包含越界 ID 测试后端拒绝。

        Returns:
            dict: 含调用方给定引用的 needs_human 测试报告。
    """
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
    """
        构造符合模型外壳协议的工具请求替身。

        Args:
            name: 工具注册名，也可填禁止动作以验证白名单拒绝。
            arguments: 该工具的参数字典，编码为 function.arguments JSON 字符串。
            call_id: 本次请求的协议 ID，默认 call-1；后续轮次需自行避免重复。

        Returns:
            dict: 一次助手工具调用和固定的测试用量计数。
    """
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


@pytest.mark.parametrize("submits_report", [True, False])
def test_last_request_reserves_the_report(runtime, submits_report):
    store, observer, incident = runtime
    requests = []

    def complete(messages, tools):
        requests.append(tools)
        if len(requests) == 1:
            names = {tool["function"]["name"] for tool in tools}
            assert not names & {"get_application_contract", "update_working_memory"}
            return assistant_call("get_application_contract", {})
        evidence = json.loads(messages[-1]["content"])["evidence_id"]
        if submits_report:
            return assistant_call("submit_report", report([evidence]), "final")
        return assistant_call("search_logs", {"keyword": "already examined"}, "over-budget")

    run = investigate(store, observer, incident["id"], complete, "test-model", max_model_calls=2)
    assert run["state"] == "COMPLETED"
    assert run["report"]["status"] == "needs_human"
    assert len(requests) == 2
    final_tool, = requests[-1]
    assert final_tool["function"]["name"] == "submit_report"
    assert "最后一次" in final_tool["function"]["description"]
    executed = [entry["tool"] for entry in store.trace(run["id"]) if entry["type"] == "tool"]
    assert executed == (["get_application_contract", "submit_report"] if submits_report else [
        "get_application_contract",
    ])


def test_manual_investigation_stops_after_three_empty_log_checks(runtime):
    store, _, incident = runtime
    offered = []

    class Observer:
        app_id = "pipeline"

        def search_logs(self, keyword, limit, since):
            return {"rows": [], "truncated": False}

    def complete(_messages, tools):
        names = {tool["function"]["name"] for tool in tools}
        offered.append(names)
        if names == {"submit_report"}:
            return assistant_call("search_logs", {}, "ignored-final")
        return assistant_call("search_logs", {}, f"logs-{len(offered)}")

    run = investigate(store, Observer(), incident["id"], complete, "test-model")
    assert len(offered) == 4 and offered[-1] == {"submit_report"}
    assert run["state"] == "COMPLETED" and run["report"]["status"] == "needs_human"
    manifest = ContextSnapshots(store, run["id"]).get(4)["manifest"]
    assert manifest["report_only"] == "no_progress" and manifest["stalled_tool_batches"] == 3
    assert manifest["model_budget"] == {
        "limit": 8, "claimed": 4, "remaining": 4, "reserved_for_parent_report": 0,
    }


def test_progress_uses_observation_content_across_reads_and_dispatches():
    first_log = {"at": "t1", "message": "started"}
    second_log = {"at": "t2", "message": "waiting"}
    log = {"runtime_id": "r1", "rows": [first_log, second_log]}
    evidence = {
        identifier: {
            "id": identifier, "tool": "search_logs", "raw": {"status": "ok", "data": log},
        } for identifier in ("ev-1", "ev-2")
    }
    recovery = InvestigationRecovery(evidence.__getitem__)

    assert recovery.record_progress("search_logs", {"status": "ok", "data": log})
    assert not recovery.record_progress("search_logs", {
        "status": "ok", "data": {**log, "rows": [second_log]},
    })
    assert not recovery.record_progress("read_evidence", {"status": "ok", **evidence["ev-1"]})
    assert not recovery.record_progress("dispatch_investigation", {
        "status": "ok", "findings": [{"support_ids": ["ev-2"]}],
    })
    assert recovery.record_progress("search_logs", {
        "status": "ok", "data": {**log, "rows": [{**second_log, "at": "t3"}]},
    })

    def metrics(time, age):
        return {"status": "ok", "data": {
            "window_end": time, "current": {"observed_at": time, "heartbeat_at": time},
            "processed_count": 25, "in_flight_processing_age_ms": age,
        }}

    assert recovery.record_progress("query_metrics", metrics("t1", 5000))
    assert not recovery.record_progress("query_metrics", metrics("t2", 5000))
    assert recovery.record_progress("query_metrics", metrics("t3", 8000))


def test_contract_boundary():
    """
        验证路径型标识、额外命令字段、重复组件和无支持假设报告均不通过 Schema。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="../../private", components=["worker"])
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="pipeline", components=["worker"], shell="anything")
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="pipeline", components=["producer", "producer"])
    with pytest.raises(ValidationError):
        DiagnosticReport.model_validate({**report(), "status": "proposed"})


def test_evidence_scope_and_integrity(runtime):
    """
        验证原文读取同时检查运行归属与正文哈希，拒绝跨运行引用和文件篡改。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
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
    """
        并发创建同事件运行，验证数据库约束保证只产生一个活跃调查。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, _, incident = runtime

    def start():
        """
            把预期的并发启动冲突转换为可统计结果。

            Args:
                无显式参数。

            Returns:
                str | None: 竞争成功返回运行 ID，唯一约束冲突返回 None。
        """
        try:
            return store.start_run(incident["id"], "test-double")
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: start(), range(2)))
    assert sum(value is not None for value in results) == 1


def test_missing_data_is_unavailable_and_tools_are_read_only(runtime):
    """
        验证缺少适配器不冒充零速率，并拒绝未知动作、任意路径及非法观测参数。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
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
    """
        用固定模型替身验证完整历史基线的真实工具执行、原文回取及 Prompt/用量追溯。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, demo, incident = runtime

    def model_double(messages, tools):
        """
            依次请求契约和原文，检验结果后返回固定报告，不模拟诊断智能。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                tools: 可调用工具的函数定义列表，包含名称、说明和参数 Schema。

            Returns:
                dict: 固定的工具调用，或带已观察引用的升级报告消息。
        """
        assert any(item["function"]["name"] == "get_application_contract" for item in tools)
        # 替身只控制工具请求顺序，结果仍由真实网关和存储产生，覆盖原始证据的保存与回取链路。
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
    protocol = [item for item in trace if item["type"] != "model_budget"]
    assert [item["type"] for item in protocol] == ["prompt", "model", "tool", "model", "tool", "model"]
    snapshot = protocol[0]
    assert snapshot["version"] == "investigation/v1"
    assert len(snapshot["sha256"]) == 64
    # 重新对实际初始消息计算指纹，不能只检查哈希长度，否则正文与审计身份可能脱节。
    actual_hash = hashlib.sha256(
        json.dumps(snapshot["messages"], ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    assert snapshot["messages_sha256"] == actual_hash
    assert snapshot["messages"][0]["role"] == "system"
    assert snapshot["tools"][:-1] == tool_definitions()
    assert snapshot["tools"][-1]["function"]["name"] == "submit_report"
    assert protocol[1]["usage"]["prompt_tokens"] == 10


@pytest.mark.parametrize(
    "message",
    [
        {"role": "assistant", "content": json.dumps(report(["ev-" + "0" * 32]))},
        {"role": "assistant", "content": "not a JSON report"},
        {"role": "assistant", "tool_calls": [{"bad": "protocol"}]},
    ],
)
def test_invalid_model_output_never_closes_incident(runtime, message):
    """
        验证非法报告、外部证据引用和错误工具协议导致调查失败且事件保持需处理。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。
            message: 当前参数化的非法助手消息，用于覆盖错误报告、越界证据和调用协议。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, demo, incident = runtime
    run = investigate(store, demo, incident["id"], lambda *_: {"message": message}, "test-double")
    assert run["state"] == "FAILED"
    assert run["report"] is None
    assert store.incident(incident["id"])["state"] == "NEEDS_HUMAN"


def test_model_cannot_execute_an_unregistered_action(runtime):
    """
        验证模型请求重启时只能收到白名单错误，不能直接执行环境变更。

        Args:
            runtime: G0 测试夹具，包含隔离 Store、空观测适配器及已登记的 pipeline 事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, demo, incident = runtime

    def model_double(messages, _):
        """
            验证模型无法越过网关白名单执行重启。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                _: 未使用的工具定义占位参数，保留模型回调的双参数签名。

            Returns:
                dict: 首轮请求禁止动作，收到错误后返回升级报告。
        """
        # 故意请求注册表外的恢复动作，错误应返回模型修正，而不是执行动作或产生观测证据。
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
    """
        以离线 HTTP 传输验证请求地址、认证、模型参数、响应筛选及密钥隐藏。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    secret = "test-only-not-a-real-key"
    settings = Config(data_dir=tmp_path, api_key=secret)

    def handler(request):
        """
            离线检查真实客户端组装的 HTTP 请求，避免测试访问供应商服务。

            Args:
                request: HTTPX MockTransport 收到的实际请求对象，认证值仅为测试占位。

            Returns:
                httpx.Response: 含模拟用量与待过滤私有字段的 200 响应。
        """
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

    # MockTransport 检查真实 HTTP 组装路径且全程离线；再从结果和配置展示两侧检查敏感值未泄露。
    result = DeepSeekClient(settings, httpx.MockTransport(handler)).complete([], tool_definitions())
    assert "reasoning_content" not in result["message"]
    assert secret not in repr(settings)
    assert secret not in json.dumps(result)
    with pytest.raises(ModelError, match="DEEPSEEK_API_KEY"):
        DeepSeekClient(Config(data_dir=tmp_path))


def test_api_missing_key_does_not_create_a_running_investigation(tmp_path):
    """
        验证缺少模型配置时 API 返回 503 且事件不会遗留 RUNNING 调查。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    # 密钥缺失仍允许登记应用/事件；调查入口必须在创建运行前失败，不能遗留 RUNNING 记录。
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
