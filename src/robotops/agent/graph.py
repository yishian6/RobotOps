"""主调查与子调查共用的模型请求和工具调用循环。"""

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from ..observations import compact_model_observation
from ..storage import Store
from .client import AssistantMessage, DeepSeekClient, ModelError
from .context import ContextSnapshots, Strategy, assemble
from .recovery import OPTIONAL_TOOLS, InvestigationRecovery


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
        recovery = InvestigationRecovery(initial_evidence_ids) if deadline is None else None

        def call_model(state: InvestigationState) -> InvestigationState:
            _remaining_time(deadline)
            history = state["messages"]
            memory = memory_snapshot()
            outgoing, manifest = assemble(history, memory, self.context_strategy)
            available = tools
            closing = recovery if recovery is not None and recovery.failed_tool else None
            if closing is not None:
                available = [tool for tool in tools if tool["function"]["name"] == final_tool]
                manifest["report_only"] = closing.failed_tool

            # 先保存实际发送视图，网络错误也能对应到输入。
            sequence = self.snapshots.save(outgoing, available, memory, {**manifest, **tags})
            try:
                message = self._request_message(outgoing, available, tags, sequence, deadline)
            except ModelError:
                if closing is None:
                    raise
                return {"messages": history, "report": closing.fallback_report()}

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
                return {"messages": history, "report": closing.fallback_report()}

            report = None
            if not calls:
                try:
                    report = finalize(message.get("content") or "")
                except ModelError:
                    if closing is None:
                        raise
                    report = closing.fallback_report()
            elif closing is not None and (
                len(calls) != 1 or calls[0]["function"]["name"] != final_tool
            ):
                report = closing.fallback_report()
            return {"messages": [*history, message], "report": report}

        def run_tools(state: InvestigationState) -> InvestigationState:
            history = list(state["messages"])
            calls = history[-1]["tool_calls"]
            report = None
            closing = recovery if recovery is not None and recovery.failed_tool else None
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
                elif recovery is not None and recovery.failed_tool and name in OPTIONAL_TOOLS:
                    result = {
                        "status": "blocked",
                        "reason": (
                            "Finish the report with available evidence "
                            "before requesting more optional work."
                        ),
                    }
                else:
                    result = compact_model_observation(name, execute(name, arguments))

                if recovery is not None:
                    failed_tool = recovery.failed_tool
                    result = recovery.record(name, result)
                    if not failed_tool and recovery.failed_tool:
                        self.store.trace(self.run_id, {"type": "report_only", **tags, "tool": name})
                self.store.trace(self.run_id, {
                    "type": "tool", **tags, "tool": name, "arguments": arguments, "result": result,
                })
                history.append({
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(result, ensure_ascii=False),
                })
            if closing is not None and report is None:
                report = closing.fallback_report()
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

        self.store.trace(self.run_id, {
            "type": "prompt", **tags, **metadata, "messages": messages, "tools": tools,
        })
        # 由分支截止时间和调查收尾规则结束运行，避免框架默认步数提前截断。
        result = graph.compile().invoke(
            {"messages": messages, "report": None}, {"recursion_limit": sys.maxsize},
        )
        _remaining_time(deadline)
        return result["report"]
