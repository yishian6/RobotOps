"""只读故障调查状态图；模型选择工具，后端校验报告及证据。"""

import json
import sys
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from ..models import DiagnosticReport
from ..observations import ObservationSource
from ..storage import Store
from ..tools import ToolGateway, tool_definitions
from .client import AssistantMessage, ModelError
from .context import ContextSnapshots, Strategy, assemble
from .memory import MemoryStore
from .prompt_loader import InvestigationPrompt, investigation_messages


class InvestigationState(TypedDict):
    """单次图执行的规范历史；上下文装配只生成发送视图，不改写此历史。

    messages 按协议排列 system/user/assistant/tool 消息；节点返回替换后的完整列表。
    report 在调查期间为 None，只有最终报告通过校验后才填入结构化字典。
    """

    messages: list[dict]
    report: dict | None


def investigate(
    store: Store,
    observer: ObservationSource | None,
    incident_id: str,
    complete,
    model_name: str,
    prompt: InvestigationPrompt | None = None,
    context_strategy: Strategy = "evidence",
):
    """为已有事件创建一次只读调查，并返回最终的持久化运行记录。

    incident_id 绑定待调查事件；complete 是接收 messages/tools 的模型调用函数。
    model_name 用于记录本次所选模型，prompt 是应用启动时验证并固定的文本快照。
    模型可以反复请求只读工具，最终报告须满足数据 Schema、证据归属及目标范围。

    事件/模板读取和运行创建失败直接抛给调用者；图执行中的异常会记录为 FAILED。
    COMPLETED 表示调查报告已经生成，不能据此认为实际故障已恢复。
    """
    incident = store.incident(incident_id)
    # 先读取资源再创建运行记录；模板缺失不能遗留 RUNNING 状态。
    messages, prompt_metadata = investigation_messages(
        incident, store.contract(incident["app_id"]), prompt
    )
    run_id = store.start_run(incident_id, model_name)
    gateway = ToolGateway(store, observer, run_id)
    tools = tool_definitions()
    memory_store = MemoryStore(store, run_id)
    snapshots = ContextSnapshots(store, run_id)

    def call_model(state):
        """追加一次模型响应；若没有工具调用，将内容作为最终诊断报告校验。"""
        memory = memory_store.current()
        outgoing, manifest = assemble(state["messages"], memory, context_strategy)
        # 保存实际输入再请求；网络失败也有对应快照。规范历史始终另外保留。
        snapshots.save(outgoing, tools, memory, manifest)
        try:
            reply = complete(outgoing, tools)
        except ModelError as exc:
            store.trace(run_id, {"type": "model_error", "usage": exc.usage, "error": str(exc)})
            raise
        # 无效响应也可能产生费用，但不能为记录用量而把未经筛选的响应正文写进轨迹。
        event = {
            "type": "model",
            "usage": reply.get("usage", {}),
            "model": reply.get("model", model_name),
        }
        try:
            message = AssistantMessage.model_validate(reply["message"]).model_dump(
                exclude_none=True
            )
        except ValidationError as exc:
            store.trace(run_id, {**event, "message": None, "invalid_message": True})
            raise ModelError("Invalid model tool-call message") from exc
        store.trace(run_id, {**event, "message": message})
        calls = message.get("tool_calls") or []
        if message.get("role") != "assistant" or not isinstance(calls, list):
            raise ModelError("Invalid model message")
        ids = [call["id"] for call in calls]
        # 工具结果依靠调用 ID 配对；同轮或跨轮重复都会让历史含义变得不明确。
        past_ids = {call["id"] for item in state["messages"] for call in item.get("tool_calls", [])}
        if len(set(ids)) != len(ids) or set(ids) & past_ids:
            raise ModelError("Duplicate tool call IDs")
        report = None
        if not calls:
            try:
                validated = DiagnosticReport.model_validate_json(message.get("content") or "")
            except ValidationError as exc:
                raise ModelError("Model final report does not match the diagnostic schema") from exc
            # Prompt 只引导模型；运行归属与动作目标仍须由后端确定性校验。
            if not validated.referenced_evidence() <= store.evidence_ids(run_id):
                raise ModelError("Model cited evidence outside this investigation")
            # 历史经验只能帮助形成假设，不能单独证明当前实例故障。unavailable 也不是实测。
            for hypothesis in validated.hypotheses:
                if hypothesis.assessment == "unverified":
                    continue
                observations = [store.read_evidence(run_id, eid) for eid in hypothesis.evidence_ids]
                if not any(
                    item["tool"] not in {"retrieve_cases", "get_application_contract"}
                    and item["raw"].get("status") == "ok"
                    for item in observations
                ):
                    raise ModelError("A confirmed hypothesis requires current observation evidence")
            target = validated.recommended_action.target
            if target and target not in store.contract(incident["app_id"])["components"]:
                raise ModelError("Model suggested a target outside the application contract")
            report = validated.model_dump(mode="json")
        return {"messages": [*state["messages"], message], "report": report}

    def run_tools(state):
        """顺序执行本轮工具请求，追加带 tool_call_id 的结果供模型下一轮读取。

        非 JSON 对象参数会变成可观察的工具错误，让模型有机会修正参数；
        工具类型不受支持属于调用协议错误，直接终止本次图执行。
        所有实际调用统一经过网关，不将函数名反射为任意 Python 函数。
        """
        messages = list(state["messages"])
        for call in messages[-1]["tool_calls"]:
            if call.get("type") != "function":
                raise ModelError("Unsupported tool call type")
            name = call["function"]["name"]
            try:
                arguments = json.loads(call["function"]["arguments"])
                if not isinstance(arguments, dict):
                    raise ValueError("Tool arguments must be an object")
            except (ValueError, TypeError):
                arguments = None
            result = (
                gateway.execute(name, arguments)
                if arguments is not None
                else {
                    "status": "error",
                    "error": "Tool arguments must be a JSON object",
                }
            )
            store.trace(
                run_id, {"type": "tool", "tool": name, "arguments": arguments, "result": result}
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(result, ensure_ascii=False),
                }
            )
        return {"messages": messages}

    # START → model → tools → model；模型不再请求工具时校验最终报告并结束。
    graph = StateGraph(InvestigationState)
    graph.add_node("model", call_model)
    graph.add_node("tools", run_tools)
    graph.add_edge(START, "model")
    graph.add_conditional_edges(
        "model",
        lambda state: "tools" if state["messages"][-1].get("tool_calls") else END,
    )
    graph.add_edge("tools", "model")
    try:
        # 保存实际消息与工具定义的快照，模板修改后仍可复核历史调查的输入。
        store.trace(
            run_id, {"type": "prompt", **prompt_metadata, "messages": messages, "tools": tools}
        )
        # LangGraph 默认 25 个节点步数会让正常长调查在约 12 轮后停止。
        # 框架要求正整数，取平台最大值以移除此隐式配额；不拿减少调查步数作为降本手段。
        result = graph.compile().invoke(
            {"messages": messages, "report": None}, {"recursion_limit": sys.maxsize}
        )
        store.finish_run(run_id, report=result["report"])
    except Exception as exc:
        # G0 不执行恢复；异常只标记调查失败并升级，不能把“流程结束”当作故障消失。
        error = (
            (str(exc).strip() or type(exc).__name__)
            if isinstance(exc, ModelError)
            else f"Investigation stopped: {type(exc).__name__}"
        )
        store.trace(run_id, {"type": "error", "error": error})
        store.finish_run(run_id, error=error)
    return store.run(run_id)
