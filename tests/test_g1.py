"""
    G1 的可复现检查：检索适用范围、证据保真、请求协议及故障用量，不评测 LLM 智能。
"""

import hashlib
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
from robotops.agent.supervision import RoleScope
from robotops.agent.workflow import investigate
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.observations import compact_model_observation
from robotops.retrieval import CaseCreate, CaseLibrary
from robotops.storage import Store
from robotops.tools import ToolGateway


@pytest.fixture
def investigation(tmp_path):
    """
        构建用于检索、记忆、上下文边界测试的单应用调查夹具。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            tuple[Store, ApplicationContract, dict]: 隔离存储、带 planner 上游依赖的契约及 OPEN 事件。
    """
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
    """
        案例时间相对当前时刻，避免测试依赖系统日期或本地时区。

        Args:
            **changes: 待覆盖的案例字段，用于构造错版本、过期、草稿等受控输入。

        Returns:
            dict: 可供 CaseCreate 校验的默认案例字段及调用方覆盖值。
    """
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
    """
        验证 FTS 检索先做版本/组件/时间过滤，保留来源，并支持依赖上游与案例撤回。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, contract, incident = investigation
    library = CaseLibrary(store)
    # 无效候选数量刻意超过 limit，验证过滤发生在截断前；草稿、禁用、过期、未来验证均应排除。
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
    # planner 自身不匹配案例，但显式上游 sensor 匹配，验证网关扩展依赖范围而非放宽应用边界。
    upstream = ToolGateway(store, None, run_id).execute(
        "retrieve_cases", {"query": "输入断流", "components": ["planner"]}
    )
    assert upstream["data"]["cases"][0]["id"] == valid["id"]
    assert store.read_evidence(run_id, result["evidence_id"])["raw"]["data"] == result["data"]
    # 撤回只影响后续检索；旧调查的来源快照仍保留当时内容，不能随当前案例状态被改写。
    library.disable(valid["id"])
    assert library.search(contract.model_dump(), "断流") == []
    assert store.read_evidence(run_id, result["evidence_id"])["raw"]["data"]["cases"][0]["enabled"]


def test_memory_scope_and_monotonic_counter_evidence(investigation):
    """
        验证记忆引用范围、反证只增保护和并发更新版本，拒绝另一运行的证据。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
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
    # 故意提交不含旧假设的新记忆，验证历史反证的保护不依赖模型继续复述它。
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


def test_supervisor_can_only_write_memory_with_visible_evidence(investigation):
    store, _, incident = investigation
    run_id = store.start_run(incident["id"], "fixture")
    visible = store.save_evidence(run_id, "query_metrics", {"status": "ok"})
    unshared = store.save_evidence(run_id, "inspect_process", {"status": "ok"})
    scope = RoleScope(ToolGateway(store, None, run_id), "supervisor", [visible])

    denied = scope.execute("update_working_memory", {"critical_ids": [unshared]})
    assert denied == {"status": "error", "error": "Memory cites evidence outside this agent's scope"}
    assert MemoryStore(store, run_id).current()["version"] == 0

    accepted = scope.execute("update_working_memory", {"critical_ids": [visible]})
    assert accepted == {"status": "ok", "version": 1, "protected_ids": [visible]}
    assert MemoryStore(store, run_id).current()["memory"]["critical_ids"] == [visible]


def test_log_query_reuses_complete_scan_and_can_read_fresh_logs(investigation):
    store, _, incident = investigation
    run_id = store.start_run(incident["id"], "fixture")

    class Observer:
        app_id = "navigation"
        calls = 0

        def search_logs(self, keyword, limit, since):
            self.calls += 1
            return {"rows": [{"message": f"log-{self.calls}"}], "truncated": self.calls == 1}

    observer = Observer()
    gateway = ToolGateway(store, observer, run_id)
    first = gateway.execute("search_logs", {})
    assert first["data"]["truncated"]
    complete = gateway.execute("search_logs", {})
    previous = gateway.execute("search_logs", {"keyword": "error"})
    assert previous["view"] == "previous_complete_log_scan"
    assert previous["evidence_id"] == complete["evidence_id"]
    assert previous["observed_at"] == complete["observed_at"]
    assert observer.calls == len(store.evidence_ids(run_id)) == 2
    assert "data" not in previous
    assert gateway.execute("search_logs", {"refresh": True})["status"] == "error"
    old_start = datetime.fromisoformat(complete["observed_at"]) - timedelta(seconds=1)
    assert gateway.execute("search_logs", {"since": old_start.isoformat()}) == previous

    scoped = gateway.execute("search_logs", {"keyword": "error"}, visible_evidence=set())
    recent = gateway.execute("search_logs", {"since": complete["observed_at"]})
    assert gateway.execute("search_logs", {"keyword": "processor"})["evidence_id"] == (
        complete["evidence_id"]
    )
    assert observer.calls == len(store.evidence_ids(run_id)) == 4
    assert scoped["data"]["rows"] == [{"message": "log-3"}]
    assert recent["data"]["rows"] == [{"message": "log-4"}]


def tool_turn(messages, number, name, result):
    """
        构造合法的完整批次，支持同轮多个工具时的原样保留检查。

        Args:
            messages: 当前测试的规范消息历史，将原地追加两条消息。
            number: 本轮编号，用于生成 call- 前缀 ID。
            name: 当前固定调用的工具名称。
            result: 由用例预先指定的可序列化工具结果。

        Returns:
            None: 原地追加唯一编号的助手调用及其工具回复。
    """
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
    """
        验证精确聚合不会合并不同数值/组件/实例，带时区时间按真实时刻排序。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    first = {
        "at": "2026-11-17T08:00:00+08:00",
        "component": "sensor",
        "instance_id": "a",
        "message": "timeout",
        "elapsed_ms": 5,
        "unit": "ms",
    }
    # 仅时间不同的两条应合并；数值、实例、组件任一变化都必须分组，不能靠消息文本判重。
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
    # 坏时间应保留原始行供排查，不能生成看似完整却排序错误的聚合结果。
    assert aggregate_logs({"rows": [{"at": "bad-time", "message": "x"}]}) == {
        "rows": [{"at": "bad-time", "message": "x"}]
    }


def test_context_protects_raw_and_refetch_and_keeps_protocol(investigation):
    """
        验证三策略的证据保护、旧实例标记、依赖范围、最新原文完整保留及工具配对。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    _, contract, _ = investigation
    messages = [
        {"role": "system", "content": "fixture"},
        {"role": "user", "content": canonical({"contract": contract.model_dump()})},
    ]
    eid = "ev-" + "1" * 32
    # 大批重复行让压缩路径有机会生效；显式更换会话和组件实例，验证旧证据不会被当作当前观测。
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
        compact_model_observation("query_metrics", {
            "status": "ok",
            "evidence_id": "ev-" + "2" * 32,
            "data": {
                "runtime_id": "new",
                "rate": 0,
                "current": {"components": {"sensor": {
                    "telemetry": {
                        "component": "sensor", "instance_id": "sensor-new", "bound_port": 9000,
                    },
                }}},
            },
        }),
    )
    # 使用实际精简路径，验证前后实例仍能比较，且非身份字段确实已从模型视图省去。
    assert json.loads(messages[-1]["content"])["view"] == "selected_fields"
    memory = {
        "version": 1,
        "memory": WorkingMemory(phase="investigation", focus_components=["planner"]).model_dump(),
        "protected_ids": [eid],
    }
    # 保护优先级适用于两种压缩策略；自适应回退也必须保留解释旧实例的候选清单。
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
    # 回取旧实例原文后，最新批次必须完整送达；回取行为也不能把当前实例倒拨成旧实例。
    tool_turn(messages, 3, "read_evidence", {"id": eid, "raw": old, "sha256": "fixture"})
    outgoing, _ = assemble(messages, memory)
    assert outgoing[-2:] == messages[-2:]  # 回取原文不再立即压缩。
    assert protocol_valid(outgoing)
    assert messages[3]["content"] == canonical(old)  # 规范历史不被就地改写。


def test_context_folds_only_explicitly_memorized_or_duplicate_observations():
    """
        验证只有明确被记忆覆盖或精确重复的观测才折叠为引用，短历史允许回退。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    messages = [{"role": "system", "content": "fixture"}, {"role": "user", "content": "fixture"}]
    # 两条 rate 相同但证据身份/采集时间不同，第三条数值不同，分别检查记忆覆盖、精确去重和新值保留。
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


def test_context_replaces_old_memory_snapshots_with_version_reference():
    """
        验证旧的工作记忆提交只保留版本引用，不在每轮归档里重复完整记忆正文。

        旧实现让 update_working_memory 的工具结果原样进入归档；该结果本身是完整记忆，
        会随事实数量逐轮累积。当前记忆已由历史头部下发，因此旧版本只需要版本号。
        用重复日志制造足够压缩空间，避免短历史体积回退掩盖该行为。

        Args:
            无显式参数。

        Returns:
            None: 归档含版本引用且不含记忆正文，当前记忆仍在头部，体积确有下降。
    """
    messages = [{"role": "system", "content": "fixture"}, {"role": "user", "content": "fixture"}]
    rows = [
        {
            "at": f"2026-11-17T01:00:{index % 60:02d}+00:00",
            "component": "processor",
            "instance_id": "instance-1",
            "message": "waiting input",
        }
        for index in range(80)
    ]
    latest = None
    for version in range(1, 5):
        tool_turn(
            messages,
            version * 2 - 1,
            "search_logs",
            {
                "status": "ok",
                "evidence_id": f"ev-{version:032x}",
                "data": {"runtime_id": "runtime-1", "rows": rows},
            },
        )
        latest = {
            "version": version,
            "protected_ids": [],
            "memory": WorkingMemory(
                phase="investigation",
                focus_components=["processor"],
                facts=[
                    {
                        "statement": f"第 {version} 轮事实 {index}",
                        "evidence_ids": ["ev-" + "a" * 32],
                    }
                    for index in range(version)
                ],
            ).model_dump(),
        }
        tool_turn(
            messages,
            version * 2,
            "update_working_memory",
            {"status": "ok", **latest},
        )
    outgoing, manifest = assemble(messages, latest)
    assert manifest["selected_view"] == "assembled"
    archive = json.loads(outgoing[2]["content"])["archive"]
    references = [entry for entry in archive if entry["tool"] == "update_working_memory"]
    assert references
    assert all(entry["view"] == "memory_version_reference" for entry in references)
    assert all("memory" not in entry and "result" not in entry for entry in references)
    assert [entry["version"] for entry in references] == [1, 2, 3]
    header = json.loads(outgoing[2]["content"])
    assert header["memory"] == latest
    assert manifest["request_bytes"] < manifest["source_bytes"]


def test_snapshot_roundtrip_scope_and_integrity(investigation):
    """
        验证快照独立保存、序号边界和正文损坏检测，不以可变外部对象充当历史输入。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, _, incident = investigation
    run_id = store.start_run(incident["id"], "fixture")
    snapshots = ContextSnapshots(store, run_id)
    original = [{"role": "system", "content": "固定规则"}, {"role": "user", "content": "原始请求"}]
    tools = [{"name": "shared_tool"}]
    sequence = snapshots.save(original, tools, {}, {"strategy": "full"})
    original[1]["content"] = "外部改写"
    second = snapshots.save(original, tools, {}, {"strategy": "full"})
    assert snapshots.get(sequence)["messages"][1]["content"] == "原始请求"
    assert snapshots.get(second)["tools"] == tools
    assert len(snapshots.list()) == 2
    with pytest.raises(KeyError):
        snapshots.get(3)
    assert snapshots.save(original, tools, {}, {"strategy": "full"}) == 3
    with store.connection() as db:
        assert db.execute("SELECT count(*) FROM context_inputs").fetchone()[0] == 1
        assert all("shared_tool" not in row[0] for row in db.execute("SELECT body FROM context_snapshots"))
        assert db.execute("SELECT count(*) FROM context_messages").fetchone()[0] == 2
        inline = json.loads(db.execute(
            "SELECT body FROM context_snapshots WHERE run_id=? AND sequence=?", (run_id, second),
        ).fetchone()[0])
        inline.pop("message_ids")
        inline["messages"] = original[1:]
        db.execute("UPDATE context_snapshots SET body=? WHERE run_id=? AND sequence=?", (
            canonical(inline), run_id, second,
        ))
    # 同一数据库内可读取旧的内联消息和新的消息引用。
    assert snapshots.get(second)["messages"] == snapshots.get(3)["messages"] == original
    with store.connection() as db:
        key = hashlib.sha256(canonical(original[1]).encode()).hexdigest()
        db.execute("UPDATE context_messages SET body='{}' WHERE sha256=?", (key,))
    with pytest.raises(ValueError, match="integrity"):
        snapshots.get(3)
    assert snapshots.get(second)["messages"] == original


def test_workflow_uses_memory_and_context_beyond_framework_default(investigation):
    """
        通过 17 轮固定响应验证长调查、记忆保护和逐轮精确上下文保存。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, _, incident = investigation
    captured, evidence_id = [], None

    def model(messages, tools):
        """
            驱动 17 轮确定性交互以验证上下文基础设施，不测量模型故障推断能力。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                tools: 可调用工具的函数定义列表，包含名称、说明和参数 Schema。

            Returns:
                dict: 随闭包轮次请求契约、更新记忆，最后返回固定升级报告和用量。
        """
        nonlocal evidence_id
        captured.append(messages)
        turn = len(captured)
        if turn == 1:
            names = {tool["function"]["name"] for tool in tools}
            assert "get_application_contract" not in names
            assert "update_working_memory" in names
            assert json.loads(messages[1]["content"])["contract"]["app_id"] == "navigation"
        result = json.loads(messages[-1]["content"]) if messages[-1]["role"] == "tool" else {}
        # 第一轮先取得真实证据身份，第二轮才引用它写记忆，避免通过虚构 ID 绕开归属校验。
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

    # 17 轮超过框架默认节点上限；逐轮核对持久快照和实际输入，证明装配结果确实用于请求。
    run = investigate(
        store, None, incident["id"], model, "fixture", context_strategy="evidence", max_model_calls=17,
    )
    assert run["state"] == "COMPLETED" and len(captured) == 17
    snapshots = ContextSnapshots(store, run["id"])
    assert len(snapshots.list()) == 17
    for index, outgoing in enumerate(captured, 1):
        assert snapshots.get(index)["messages"] == outgoing and protocol_valid(outgoing)
    assert snapshots.get(17)["memory"]["protected_ids"] == [evidence_id]
    with store.connection() as db:
        latest = json.loads(db.execute(
            "SELECT body FROM context_snapshots WHERE run_id=? AND sequence=17", (run["id"],),
        ).fetchone()[0])
        assert latest["memory_version"] == 1
        assert "memory" not in latest
    events = store.trace(run["id"])
    write = next(item for item in events if item.get("tool") == "update_working_memory")
    assert write["arguments"]["critical_ids"] == [evidence_id]
    assert write["result"] == {"status": "ok", "version": 1, "protected_ids": [evidence_id]}
    assert any(
        e["view"] == "duplicate_reference" for e in snapshots.get(17)["manifest"]["evidence"]
    )


def manual_report():
    """
        构建 G1 编排与失败路径复用的最小协议报告。

        Args:
            无显式参数。

        Returns:
            dict: 不包含根因确认的合法 needs_human 报告。
    """
    return {
        "status": "needs_human",
        "summary": "仅做协议测试",
        "hypotheses": [],
        "recommended_action": {"kind": "manual_review", "reason": "缺少当前实测"},
    }


@pytest.mark.parametrize("invalid_protocol", [False, True])
def test_api_cases_usage_snapshots_and_invalid_model_usage(tmp_path, invalid_protocol):
    """
        验证案例与快照接口、非法响应计费用量保留以及供应商私有字段不进入轨迹。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。
            invalid_protocol: True 注入错误调用协议与私有字段；False 只返回不合法的最终报告。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """

    def model(*_):
        """
            产生可计费但无效的模型响应，供验证审计过滤与失败用量累计。

            Args:
                *_: 被忽略的位置参数元组，兼容模型回调传入的 messages/tools。

            Returns:
                dict: 取决于外层参数，返回非法报告或带私有字段的错误协议，同时给出固定用量。
        """
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
        # 诊断失败也可能产生费用，且无效协议中的私有字段不能为记录用量而进入公开轨迹。
        usage = client.get(f"/runs/{run['id']}/usage").json()
        assert usage["reported_tokens"] == {"prompt_tokens": 123, "completion_tokens": 9}
        assert usage["reported_cache_tokens"] == dict.fromkeys(("prompt_cache_hit_tokens", "prompt_cache_miss_tokens"))
        assert usage["missing_cache_usage"] == {"prompt_cache_hit_tokens": 1, "prompt_cache_miss_tokens": 1}
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
    """
        构造带用量但缺少有效选择项的 HTTP 响应，验证失败调用仍保留已知计费计数。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
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
    """
        验证仅引用历史检索结果的确定性根因报告会被拒绝，不当作当前实例实测。

        Args:
            investigation: G1 测试夹具，包含隔离 Store、navigation 应用契约及异常事件。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store, _, incident = investigation
    calls = 0

    def model(messages, _):
        """
            故意提交证据类型不合格的当前根因结论以验证后端拒绝。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                _: 未使用的工具定义参数，保留模型回调签名。

            Returns:
                dict: 首轮历史检索请求，随后给出故意只引用案例的 proposed 报告。
        """
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
        # 引用属于本轮但来源只是历史案例，故意构造结构合法的报告来验证来源类型检查。
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
    """
        验证离线对照公开全部失败与无收益场景，且不冒充真实 Token 或诊断质量评测。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
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
    """
        验证循环/越界组件依赖和未知上下文策略均在输入边界失败。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    with pytest.raises(ValidationError):
        ApplicationContract(
            app_id="app", components=["one", "two"], dependencies={"one": ["two"], "two": ["one"]}
        )
    with pytest.raises(ValidationError):
        ApplicationContract(app_id="app", components=["one"], dependencies={"one": ["other"]})
    with pytest.raises(ValueError):
        Config(context_strategy="unknown")
