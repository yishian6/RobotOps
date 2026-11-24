"""
    只读故障调查状态图；模型选择工具，后端校验报告及证据。
"""

import json
import sys
from datetime import datetime, timezone
from time import monotonic
from typing import Literal, TypedDict, get_args

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from ..config import Config
from ..models import DiagnosticReport
from ..observations import ObservationSource, compact_model_observation
from ..storage import Store
from ..tools import ToolGateway, tool_definitions
from .client import AssistantMessage, DeepSeekClient, ModelError
from .context import ContextSnapshots, Strategy, assemble
from .memory import MemoryStore
from .prompt_loader import InvestigationPrompt, investigation_messages, load_investigation_prompt
from .recovery import OPTIONAL_TOOLS, InvestigationRecovery
from .supervision import RolePrompts, Supervision, load_role_prompts

InvestigationMode = Literal["single", "supervised"]
INVESTIGATION_MODES = get_args(InvestigationMode)
DEFAULT_API_MODE: InvestigationMode = "single"
SUBMIT_REPORT = "submit_report"
_REPORT_INSTRUCTION = (
    "先查能区分故障假设的现场观测。初始消息已有应用契约。历史案例和工作记忆按需使用。"
    "能说明故障现象、范围和下一步时，单独通过 submit_report 提交，未证实的机制列为待查。"
)


class InvestigationState(TypedDict):
    """
        单次图执行的规范历史；上下文装配只生成发送视图，不改写此历史。

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
    mode: InvestigationMode = DEFAULT_API_MODE,
    role_prompts: RolePrompts | None = None,
    *,
    branch_timeout_seconds: float = Config.branch_timeout_seconds,
    automatic: bool = False,
):
    """
        为已有事件创建一次只读调查，并返回最终的持久化运行记录。

        incident_id 绑定待调查事件；complete 是接收 messages/tools 的模型调用函数。
        model_name 用于记录本次所选模型，prompt 是应用启动时验证并固定的文本快照。
        模型可以反复请求只读工具，最终报告须满足数据 Schema、证据归属及目标范围。

        事件/模板读取和运行创建失败直接抛给调用者；图执行中的异常会记录为 FAILED。
        COMPLETED 表示调查报告已经生成，不能据此认为实际故障已恢复。

        Args:
            store (Store): 业务存储实例，提供当前应用、运行记录、证据及 SQLite 事务连接。
            observer (ObservationSource | None): 当前应用的只读观测适配器；为 None 时环境观测返回不可用。
            incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。
            complete: 必需的模型回调，接收本轮 messages/tools；客户端的选择与密钥配置由调用方完成。
            model_name (str): 本次使用的模型名称，只用于调用配置或运行审计，不用于推断诊断质量。
            prompt (InvestigationPrompt | None): 已加载并固定的 Prompt 快照；为 None 时读取默认版本。
            context_strategy (Strategy): 上下文装配策略，作用于每个独立 Agent 历史。
            mode (InvestigationMode): 单 Agent 调查或层级式调查。
            role_prompts (RolePrompts | None): 可复用服务启动时读取的角色指令快照。
            branch_timeout_seconds: 一个子任务批次允许等待的时长，主 Agent 调查可继续。
        automatic: 同事务领取与事件绑定的自动调查任务。

        Returns:
            dict: 已完成或失败的持久化运行记录；图内异常记录为 FAILED 后返回。
    """
    incident = store.incident(incident_id)
    contract = store.contract(incident["app_id"])
    # 先校验模式并读取资源，避免配置错误留下 RUNNING 记录。
    if mode not in INVESTIGATION_MODES:
        raise ValueError("Unknown investigation mode")
    report_instruction = _REPORT_INSTRUCTION
    if mode == "supervised":
        role_prompts = role_prompts or load_role_prompts()
        report_instruction += "\n" + role_prompts.supervisor
    prompt = prompt or load_investigation_prompt()
    run_id = store.start_run(incident_id, model_name, automatic=automatic)

    def validate_report(content: str, visible_evidence=None) -> dict:
        """
            校验最终报告结构、可见证据、实测依据与动作目标。

            Args:
                content: 模型返回的报告 JSON。
                visible_evidence: 主 Agent 已观测或已明确接收的证据；单 Agent 可用本次运行全部证据。

            Returns:
                dict: 可以持久化的诊断报告。
        """
        try:
            validated = DiagnosticReport.model_validate_json(content)
        except ValidationError as exc:
            raise ModelError("Model final report does not match the diagnostic schema") from exc
        available = store.evidence_ids(run_id) if visible_evidence is None else visible_evidence
        references = validated.referenced_evidence()
        if not references <= available:
            raise ModelError("Model cited evidence outside this investigation")
        if trigger_evidence_id is not None and trigger_evidence_id not in references:
            raise ModelError("The report must cite the original ROS Topic observation")
        # 历史案例、应用契约和缺测不能单独证明一个当前根因。
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
        if target and target not in contract["components"]:
            raise ModelError("Model suggested a target outside the application contract")
        return validated.model_dump(mode="json")

    def run_graph(
        messages, tools, execute, finalize, memory_snapshot, tags, metadata, *,
        final_tool: str, deadline: float | None = None,
    ):
        """
            在独立的 LangGraph 状态中运行一位 Agent，复用相同的请求与工具协议。

            Args:
                messages: 本角色的初始 system/user 消息。
                tools: 当前角色允许调用的工具定义。
                execute: 实际工具执行和权限检查入口。
                finalize: 报告或领域发现的结构和证据校验。
                memory_snapshot: 提供当前角色工作记忆的回调。
                tags: 模型请求与工具轨迹的角色/任务归属。
                metadata: 当前使用的 Prompt 版本与指纹。
                final_tool: 结构化提交工具，参数通过 finalize 后可直接结束图。
                deadline: 子任务批次单调时钟截止时间；主 Agent 不受此截止时间影响。

            Returns:
                dict: 校验通过的报告或领域发现。
        """
        invalid_submissions = set()
        recovery = InvestigationRecovery() if deadline is None else None

        def remaining_time() -> float | None:
            """
                检查本轮是否还能继续，阻止已超时的分支发出新的模型调用或工具请求。

                Returns:
                    float | None: 子任务剩余秒数，主 Agent 返回 None。
            """
            if deadline is None:
                return None
            seconds = deadline - monotonic()
            if seconds <= 0:
                raise ModelError("Branch deadline elapsed")
            return seconds

        def call_model(state):
            """
                追加一次模型响应；若没有工具调用，将内容作为最终诊断报告校验。

                Args:
                    state: 当前图状态；messages 是未压缩规范历史，report 尚未完成时为 None。

                Returns:
                    dict: 新的完整规范消息历史，以及最终报告字典或 None。
            """
            # 每轮读取最新记忆装配发送视图；state 仍保存规范历史，压缩不会覆盖审计原文。
            remaining_time()
            memory = memory_snapshot()
            outgoing, manifest = assemble(state["messages"], memory, context_strategy)
            available = tools
            report_only = recovery is not None and recovery.failed_tool is not None
            if report_only:
                # 每份结果仍留在本轮上下文。收尾请求只暴露报告工具，避免再进入可选步骤。
                available = [tool for tool in available if tool["function"]["name"] == final_tool]
                manifest["report_only"] = recovery.failed_tool
            # 保存实际输入再请求；网络失败也有对应快照。规范历史始终另外保留。
            sequence = snapshots.save(outgoing, available, memory, {**manifest, **tags})
            try:
                remaining = remaining_time()
                if remaining is not None and isinstance(getattr(complete, "__self__", None), DeepSeekClient):
                    reply = complete(outgoing, available, timeout_seconds=remaining)
                else:
                    reply = complete(outgoing, available)
            except ModelError as exc:
                store.trace(run_id, {
                    "type": "model_error", **tags, "context_sequence": sequence,
                    "usage": exc.usage, "error": str(exc),
                })
                if report_only:
                    return {"messages": state["messages"], "report": recovery.fallback_report()}
                raise
            # 无效响应也可能产生费用，但不能为记录用量而把未经筛选的响应正文写进轨迹。
            event = {
                "type": "model",
                **tags,
                "context_sequence": sequence,
                "usage": reply.get("usage", {}),
                "model": reply.get("model", model_name),
            }
            # 协议字段先通过白名单模型校验再落轨迹，供应商附加字段不能绕过这一边界。
            try:
                message = AssistantMessage.model_validate(reply["message"]).model_dump(
                    exclude_none=True
                )
            except (KeyError, ValidationError) as exc:
                store.trace(run_id, {**event, "message": None, "invalid_message": True})
                if report_only:
                    return {"messages": state["messages"], "report": recovery.fallback_report()}
                raise ModelError("Invalid model tool-call message") from exc
            store.trace(run_id, {**event, "message": message})
            # 迟到的模型响应仍记用量，但不能推进图或发布发现。
            remaining_time()
            calls = message.get("tool_calls") or []
            if message.get("role") != "assistant" or not isinstance(calls, list):
                raise ModelError("Invalid model message")
            ids = [call["id"] for call in calls]
            # 工具结果依靠调用 ID 配对；同轮或跨轮重复都会让历史含义变得不明确。
            past_ids = {call["id"] for item in state["messages"] for call in item.get("tool_calls", [])}
            if len(set(ids)) != len(ids) or set(ids) & past_ids:
                if report_only:
                    return {"messages": state["messages"], "report": recovery.fallback_report()}
                raise ModelError("Duplicate tool call IDs")
            report = None
            if not calls:
                try:
                    report = finalize(message.get("content") or "")
                except ModelError:
                    if not report_only:
                        raise
                    report = recovery.fallback_report()
            elif report_only and (len(calls) != 1 or calls[0]["function"]["name"] != final_tool):
                report = recovery.fallback_report()
            return {"messages": [*state["messages"], message], "report": report}

        def run_tools(state):
            """
                顺序执行本轮工具请求，追加带 tool_call_id 的结果供模型下一轮读取。

                非 JSON 对象参数会变成可观察的工具错误，让模型有机会修正参数；
                工具类型不受支持属于调用协议错误，直接终止本次图执行。
                所有实际调用统一经过网关，不将函数名反射为任意 Python 函数。

                Args:
                    state: 当前图状态；最后一条助手消息必须包含本轮已验证的工具调用列表。

                Returns:
                dict: 追加完整工具回复，提交工具验证通过时一并返回结构化结果。
            """
            # 顺序执行同批工具，使后续调用能看见前一调用保存的证据或记忆，轨迹顺序也保持确定。
            messages = list(state["messages"])
            calls = messages[-1]["tool_calls"]
            report = None
            report_only = recovery is not None and recovery.failed_tool is not None
            for call in calls:
                remaining_time()
                if call.get("type") != "function":
                    raise ModelError("Unsupported tool call type")
                name = call["function"]["name"]
                # 无效 JSON 参数转为可见工具错误，让模型纠正；不能用 eval 或反射执行参数。
                try:
                    arguments = json.loads(call["function"]["arguments"])
                    if not isinstance(arguments, dict):
                        raise ValueError("Tool arguments must be an object")
                except (ValueError, TypeError):
                    arguments = None
                if arguments is None:
                    result = {"status": "error", "error": "Tool arguments must be a JSON object"}
                elif name == final_tool:
                    if len(calls) != 1:
                        result = {"status": "error", "error": "Submit the finding in a separate call"}
                    else:
                        try:
                            report = finalize(json.dumps(arguments, ensure_ascii=False))
                        except ModelError as exc:
                            # 校验失败仍走工具协议，让领域 Agent 修正字段或引用，无需重跑已完成取证。
                            submission = json.dumps(arguments, sort_keys=True, ensure_ascii=False)
                            if submission in invalid_submissions and not report_only:
                                raise ModelError("Repeated invalid structured submission") from exc
                            invalid_submissions.add(submission)
                            result = {"status": "error", "error": str(exc)}
                        else:
                            result = {"status": "ok"}
                elif recovery is not None and recovery.failed_tool and name in OPTIONAL_TOOLS:
                    result = {
                        "status": "blocked",
                        "reason": "Finish the report with available evidence before requesting more optional work.",
                    }
                else:
                    # 网关先保存原始证据，只精简发给 Agent 的常规观测视图。
                    result = compact_model_observation(name, execute(name, arguments))
                if recovery is not None:
                    was_closing = recovery.failed_tool is not None
                    result = recovery.record(name, result)
                    if not was_closing and recovery.failed_tool:
                        store.trace(run_id, {"type": "report_only", **tags, "tool": name})
                store.trace(
                    run_id, {"type": "tool", **tags, "tool": name, "arguments": arguments, "result": result}
                )
                # 即使工具返回 error 也必须回复原调用 ID，下一轮才能得到完整且合法的调用/结果配对。
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call["id"],
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )
            if report_only and report is None:
                report = recovery.fallback_report()
            return {"messages": messages, "report": report}

        # START → model → tools → model；模型不再请求工具时校验最终报告并结束。
        graph = StateGraph(InvestigationState)
        graph.add_node("model", call_model)
        graph.add_node("tools", run_tools)
        graph.add_edge(START, "model")
        graph.add_conditional_edges(
            "model",
            lambda state: "tools" if not state["report"] and state["messages"][-1].get("tool_calls") else END,
        )
        graph.add_conditional_edges("tools", lambda state: END if state["report"] else "model")

        store.trace(
            run_id, {
                "type": "prompt", **tags, **metadata, "messages": messages,
                "tools": tools,
            }
        )
        # 调查不以框架默认 25 步作为总调用额度；相同任务无新线索时由调查逻辑终止。
        result = graph.compile().invoke(
            {"messages": messages, "report": None}, {"recursion_limit": sys.maxsize}
        )
        remaining_time()
        return result["report"]

    try:
        # 工具、记忆和快照使用服务端生成的 run_id，角色范围在这个运行之内进一步收窄。
        gateway = ToolGateway(store, observer, run_id)
        memory_store = MemoryStore(store, run_id)
        snapshots = ContextSnapshots(store, run_id)
        trigger_evidence_id = None
        if (fact := incident.get("ros_fact")) is not None:
            observed_at = datetime.fromtimestamp(
                fact["observed_at_ms"] / 1000, timezone.utc,
            ).isoformat()
            trigger_evidence_id = store.save_evidence(run_id, "ros1_topic_observation", {
                "status": "ok", "observed_at": observed_at, "data": fact,
            })
            incident = {
                **incident, "ros_fact": {**fact, "evidence_id": trigger_evidence_id},
                "investigation_goal": (
                    "用现场证据说明 Topic 静默和可能影响。该记录仅证明采集端在给定窗口内"
                    "未收到消息，根因依据不足时保留待查。"
                ),
            }
        messages, prompt_metadata = investigation_messages(
            incident, contract, prompt, report_tool_instruction=report_instruction,
        )
        report_tool = {
            "type": "function",
            "function": {
                "name": SUBMIT_REPORT,
                "description": "提交诊断报告，只引用本次调查可见的现场证据。",
                "parameters": DiagnosticReport.model_json_schema(),
            },
        }
        if mode == "supervised":
            supervision = Supervision(
                gateway, incident, contract, role_prompts, run_graph,
                branch_timeout_seconds=branch_timeout_seconds,
            )
            if trigger_evidence_id is not None:
                supervision.main.evidence_ids.add(trigger_evidence_id)
            report = run_graph(
                messages,
                [*supervision.tools, report_tool],
                supervision.execute,
                lambda content: validate_report(content, supervision.main.evidence_ids),
                memory_store.current,
                {"agent_role": "supervisor", "investigation_mode": mode},
                {**prompt_metadata, **supervision.metadata},
                final_tool=SUBMIT_REPORT,
            )
        else:
            report = run_graph(
                messages, [*tool_definitions(), report_tool], gateway.execute, validate_report,
                memory_store.current, {"agent_role": "single", "investigation_mode": mode}, prompt_metadata,
                final_tool=SUBMIT_REPORT,
            )
        store.finish_run(run_id, report=report)
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
