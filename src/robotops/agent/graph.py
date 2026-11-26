"""主调查与子调查共用的模型请求和工具调用循环。"""

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from threading import Lock
from time import monotonic
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from ..config import ContextWindow
from ..observations import compact_model_observation
from ..storage import Store
from .client import AssistantMessage, DeepSeekClient, ModelError
from .context import ContextSnapshots, Strategy, assemble, fit_context_window
from .recovery import MAX_STALLED_ROUNDS, OPTIONAL_TOOLS, InvestigationRecovery

DEFAULT_MAX_MODEL_CALLS = 8


class ModelCallBudget:
    """Atomically share a run's request limit across the supervisor and its branches."""

    def __init__(self, limit: int):
        self._limit = limit
        self._left = limit
        self._lock = Lock()

    def claim(self, *, reserve: int) -> tuple[bool, dict[str, int]]:
        with self._lock:
            accepted = self._left > reserve
            if accepted:
                self._left -= 1
            return accepted, {
                "limit": self._limit, "claimed": self._limit - self._left,
                "remaining": self._left, "reserved_for_parent_report": reserve,
            }


class InvestigationState(TypedDict):
    """保留单个 Agent 的完整消息历史和已校验结果。"""

    messages: list[dict]
    report: dict | None


def _remaining_time(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    seconds = deadline - monotonic()
    if seconds <= 0:
        raise ModelError("Branch deadline elapsed")
    return seconds


def _tool_arguments(raw: str) -> dict | None:
    try:
        arguments = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return arguments if isinstance(arguments, dict) else None


@dataclass(frozen=True)
class InvestigationGraph:
    """共享模型和审计资源，各角色的历史和重试状态由单次 run 持有。"""

    store: Store
    run_id: str
    complete: Callable[..., dict]
    model_name: str
    context_strategy: Strategy
    snapshots: ContextSnapshots
    budget: ModelCallBudget
    context_window: ContextWindow = ContextWindow()

    def _request_message(
        self, outgoing: list[dict], tools: list[dict], tags: dict,
        sequence: int, deadline: float | None,
    ) -> dict:
        try:
            remaining = _remaining_time(deadline)
            if remaining is not None and isinstance(
                getattr(self.complete, "__self__", None), DeepSeekClient,
            ):
                reply = self.complete(outgoing, tools, timeout_seconds=remaining)
            else:
                reply = self.complete(outgoing, tools)
        except ModelError as exc:
            self.store.trace(self.run_id, {
                "type": "model_error", **tags, "context_sequence": sequence,
                "usage": exc.usage, "error": str(exc),
            })
            raise

        event = {
            "type": "model",
            **tags,
            "context_sequence": sequence,
            "usage": reply.get("usage", {}),
            "model": reply.get("model", self.model_name),
        }
        # 无效回复仍记用量，响应正文须通过协议校验才落轨迹。
        try:
            message = AssistantMessage.model_validate(reply["message"]).model_dump(
                exclude_none=True,
            )
        except (KeyError, ValidationError) as exc:
            self.store.trace(self.run_id, {**event, "message": None, "invalid_message": True})
            raise ModelError("Invalid model tool-call message") from exc
        self.store.trace(self.run_id, {**event, "message": message})
        return message

    def run(
        self,
        messages: list[dict],
        tools: list[dict],
        execute: Callable[[str, dict], dict],
        finalize: Callable[[str], dict],
        memory_snapshot: Callable[[], dict],
        tags: dict,
        metadata: dict,
        *,
        final_tool: str,
        deadline: float | None = None,
        initial_evidence_ids: tuple[str, ...] = (),
    ) -> dict:
        """运行一个 Agent，直到提交报告或子调查发现。"""
        invalid_submissions = set()
        recovery = InvestigationRecovery(
            partial(self.store.read_evidence, self.run_id), initial_evidence_ids,
        )
        is_branch = deadline is not None
        memory_enabled = any(tool["function"]["name"] == "update_working_memory" for tool in tools)
        context_sequence = 0
        closing = None

        def fallback_report(reason):
            if is_branch:
                return {
                    "status": "blocked", "blocked_reason": reason,
                    "missing_observations": ["分支尚未提交经过校验的取证结果。"],
                }
            return recovery.fallback_report(reason)

        def request_tools(reason):
            if reason is None:
                return tools
            tool, = (tool for tool in tools if tool["function"]["name"] == final_tool)
            function = tool["function"]
            instruction = {
                "model_call_budget": " 本轮是最后一次模型请求。",
                "no_progress": " 连续查询未获得新线索，请结束调查。",
                "context_window": " 上下文已达到工作窗口，请结束调查。",
            }.get(reason, "")
            return [{**tool, "function": {
                **function,
                "description": function["description"] + instruction + " 请根据已有证据"
                "提交结果，明确标出缺少实测的结论和待查项。",
            }}]

        def call_model(state: InvestigationState) -> InvestigationState:
            nonlocal closing, context_sequence
            _remaining_time(deadline)
            history = state["messages"]
            accepted, budget = self.budget.claim(reserve=1 if is_branch else 0)
            closing = recovery.failed_tool
            if not accepted:
                closing = "model_call_budget"
            elif closing is None and recovery.stalled_rounds >= MAX_STALLED_ROUNDS:
                closing = "no_progress"
            elif closing is None and budget["remaining"] == budget["reserved_for_parent_report"]:
                closing = "model_call_budget"
            audit = {"model_budget": budget, "stalled_tool_batches": recovery.stalled_rounds}
            budget_event = {
                "type": "model_budget", **tags, **audit, "accepted": accepted, "report_only": closing,
            }
            try:
                if not accepted:
                    return {"messages": history, "report": fallback_report("model_call_budget")}
                memory = memory_snapshot()
                base, manifest = assemble(history, memory, self.context_strategy)
                assembled = manifest.copy()
                available = request_tools(closing)
                outgoing, window_audit = fit_context_window(
                    history, base, available, memory, manifest, self.context_window,
                )
                if not window_audit["fits"] and closing is None:
                    initial = window_audit["before_estimated_tokens"]
                    closing = "context_window"
                    available = request_tools(closing)
                    manifest = assembled
                    outgoing, window_audit = fit_context_window(
                        history, base, available, memory, manifest, self.context_window,
                    )
                    window_audit["before_all_tools_estimated_tokens"] = initial
                if not window_audit["fits"]:
                    closing = "context_window"
                    budget_event.update(report_only=closing, context_window=window_audit)
                    return {"messages": history, "report": fallback_report(closing)}
                manifest.update(audit, context_window=window_audit)
                budget_event["report_only"] = closing
                if closing is not None:
                    manifest["report_only"] = closing

                # 先保存实际发送视图，网络错误也能对应到输入。
                context_sequence = self.snapshots.save(outgoing, available, memory, {**manifest, **tags})
                budget_event["context_sequence"] = context_sequence
                if len(history) == len(messages):
                    self.store.trace(self.run_id, {
                        "type": "prompt", **tags, **metadata, "context_sequence": context_sequence,
                    })
            finally:
                self.store.trace(self.run_id, budget_event)
            try:
                message = self._request_message(outgoing, available, tags, context_sequence, deadline)
            except ModelError:
                if closing is None:
                    raise
                return {"messages": history, "report": fallback_report(closing)}

            # 迟到回复只记用量，不能推进调查。
            _remaining_time(deadline)
            if message["role"] != "assistant":
                raise ModelError("Invalid model message")
            calls = message.get("tool_calls") or []
            ids = {call["id"] for call in calls}
            past_ids = {call["id"] for item in history for call in item.get("tool_calls", [])}
            if len(ids) != len(calls) or ids & past_ids:
                if closing is None:
                    raise ModelError("Duplicate tool call IDs")
                return {"messages": history, "report": fallback_report(closing)}

            report = None
            if not calls:
                try:
                    report = finalize(message.get("content") or "")
                except ModelError:
                    if closing is None:
                        raise
                    report = fallback_report(closing)
            elif closing is not None and (
                len(calls) != 1 or calls[0]["function"]["name"] != final_tool
            ):
                report = fallback_report(closing)
            return {"messages": [*history, message], "report": report}

        def run_tools(state: InvestigationState) -> InvestigationState:
            history = list(state["messages"])
            calls = history[-1]["tool_calls"]
            report = None
            progressed = False
            # 同批按顺序执行，让后面的调用能读取前面保存的证据和记忆。
            for call in calls:
                _remaining_time(deadline)
                if call["type"] != "function":
                    raise ModelError("Unsupported tool call type")
                function = call["function"]
                name = function["name"]
                arguments = _tool_arguments(function["arguments"])
                if arguments is None:
                    result = {"status": "error", "error": "Tool arguments must be a JSON object"}
                elif name == final_tool and len(calls) != 1:
                    result = {"status": "error", "error": "Submit the finding in a separate call"}
                elif name == final_tool:
                    try:
                        report = finalize(json.dumps(arguments, ensure_ascii=False))
                    except ModelError as exc:
                        submission = json.dumps(arguments, sort_keys=True, ensure_ascii=False)
                        if submission in invalid_submissions and closing is None:
                            raise ModelError("Repeated invalid structured submission") from exc
                        invalid_submissions.add(submission)
                        result = {"status": "error", "error": str(exc)}
                    else:
                        result = {"status": "ok"}
                elif name == "update_working_memory" and not memory_enabled:
                    result = {
                        "status": "error", "error": "Working memory is disabled for this investigation",
                        "next_step": "Use the current evidence and submit the result when ready.",
                    }
                elif recovery.failed_tool and name in OPTIONAL_TOOLS:
                    result = {
                        "status": "blocked",
                        "reason": (
                            "Finish the report with available evidence "
                            "before requesting more optional work."
                        ),
                    }
                else:
                    result = compact_model_observation(name, execute(name, arguments))

                failed_tool = recovery.failed_tool
                result = recovery.record(name, result)
                if not failed_tool and recovery.failed_tool:
                    self.store.trace(self.run_id, {"type": "report_only", **tags, "tool": name})
                progressed = recovery.record_progress(name, result) or progressed
                trace_result = result
                if result.get("evidence_id"):
                    trace_result = {key: value for key, value in result.items() if key != "data"}
                elif name == "read_evidence" and result.get("status") == "ok":
                    trace_result = {key: value for key, value in result.items() if key != "raw"}
                self.store.trace(self.run_id, {
                    "type": "tool", **tags, "tool": name, "tool_call_id": call["id"],
                    "context_sequence": context_sequence, "result": trace_result,
                })
                history.append({
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(result, ensure_ascii=False),
                })
            recovery.stalled_rounds = 0 if progressed else recovery.stalled_rounds + 1
            if closing is not None and report is None:
                report = fallback_report(closing)
            return {"messages": history, "report": report}

        graph = StateGraph(InvestigationState)
        graph.add_node("model", call_model)
        graph.add_node("tools", run_tools)
        graph.add_edge(START, "model")
        graph.add_conditional_edges(
            "model",
            lambda state: (
                "tools" if not state["report"] and state["messages"][-1].get("tool_calls") else END
            ),
        )
        graph.add_conditional_edges("tools", lambda state: END if state["report"] else "model")

        # 由分支截止时间和调查收尾规则结束运行，避免框架默认步数提前截断。
        result = graph.compile().invoke(
            {"messages": messages, "report": None}, {"recursion_limit": sys.maxsize},
        )
        _remaining_time(deadline)
        return result["report"]
