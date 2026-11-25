"""
    调查中重复校验失败或连续无进展时，已有原始证据仍可用于报告。
"""

import hashlib
import json
from collections.abc import Callable

from ..models import DiagnosticReport
from ..observations import compact_model_observation
from ..tools import DIRECT_OBSERVATION_TOOLS

OPTIONAL_TOOLS = {"update_working_memory", "dispatch_investigation"}
MAX_STALLED_ROUNDS = 3
_QUERY_TIMESTAMPS = {"observed_at", "window_start", "window_end", "elapsed_seconds", "heartbeat_at"}


def _without_sampling_times(value):
    if isinstance(value, dict):
        return {
            key: _without_sampling_times(item)
            for key, item in value.items() if key not in _QUERY_TIMESTAMPS
        }
    if isinstance(value, list):
        return [_without_sampling_times(item) for item in value]
    return value


def _observation_signatures(tool: str, data: dict | None) -> set[bytes]:
    """Sign observations individually so subsets, reordering and reminted IDs stay equivalent."""
    if not data:
        return set()
    if tool == "search_logs":
        scope = (data.get("source"), data.get("runtime_id"), data.get("process_name"))
        records = [(scope, row) for row in data.get("rows") or data.get("lines") or ()]
    elif tool == "retrieve_cases":
        records = data.get("cases") or ()
    elif tool == "get_recent_config_changes" and "changes" in data:
        records = [(data.get("runtime_id"), row) for row in data["changes"]]
    else:
        # Keep actual counters, identities, observations of faults and in-flight/output ages.
        records = [_without_sampling_times(data)]
    return {
        hashlib.sha256(json.dumps([tool, record], sort_keys=True).encode()).digest()
        for record in records
    }


def _evidence_signatures(entry: dict) -> set[bytes]:
    raw = entry["raw"]
    if raw.get("status") != "ok":
        return set()
    tool = entry["tool"]
    view = compact_model_observation(tool, {**raw, "evidence_id": entry["id"]})
    return _observation_signatures(tool, view.get("data"))


class InvestigationRecovery:
    """
        在一次主调查中记录可修正的错误和已取得的现场证据。
    """

    def __init__(self, read_evidence: Callable[[str], dict], initial_evidence_ids: tuple[str, ...] = ()):
        """
            建立独立的错误历史，保留调查触发时已取得的现场证据。

            Returns:
                None: 尚未触发收尾的状态。
        """
        self.failures: dict[str, set[tuple]] = {}
        self.evidence_ids = list(initial_evidence_ids)
        self.failed_tool: str | None = None
        self.stalled_rounds = 0
        self.observations: set[bytes] = set()
        self.read_evidence = read_evidence
        self.observations.update(self._referenced_signatures(initial_evidence_ids))

    def _referenced_signatures(self, references) -> set[bytes]:
        signatures = set()
        for evidence_id in references:
            try:
                entry = self.read_evidence(evidence_id)
            except (OSError, KeyError, ValueError):
                # A reference without readable content cannot establish new progress.
                continue
            signatures.update(_evidence_signatures(entry))
        return signatures

    def record_progress(self, tool: str, result: dict) -> bool:
        """Count meaningful results once, independently of freshly assigned evidence IDs."""
        if result.get("status") != "ok":
            return False
        if tool == "dispatch_investigation":
            signatures = self._referenced_signatures({
                evidence_id for finding in result.get("findings", [])
                for key in ("support_ids", "contradict_ids") for evidence_id in finding.get(key, [])
            })
        elif tool == "read_evidence":
            signatures = _evidence_signatures(result)
        else:
            signatures = _observation_signatures(tool, result.get("data"))
        new = not signatures.issubset(self.observations)
        self.observations.update(signatures)
        return new

    def record(self, tool: str, result: dict) -> dict:
        """
            允许纠正一次同类失败。再次遇到相同问题时，下一轮只生成报告。

            Args:
                tool: 已执行的工具名称。
                result: 工具结果，可能包含字段级错误或证据 ID。

            Returns:
                dict: 原结果，或带有收尾指引的失败结果。
        """
        status = result.get("status")
        if status == "ok":
            if tool in DIRECT_OBSERVATION_TOOLS and result.get("evidence_id"):
                self.evidence_ids.append(result["evidence_id"])
            # 纠正成功意味着这项工具已恢复，不把历史错误算到新问题上。
            self.failures.pop(tool, None)
            return result
        if self.failed_tool or tool not in OPTIONAL_TOOLS:
            return result
        signatures = set()
        if status == "error":
            for issue in result.get("validation_errors", ()):
                # 列表项顺序会随模型重写而变，错误种类和字段路径才代表是否完成纠正。
                path = tuple(part for part in issue["path"] if isinstance(part, str))
                signatures.add((path, issue["code"]))
            if not signatures:
                signatures.add(((), result["error"]))
        elif status == "skipped":
            signatures = {
                ((), item["reason"]) for item in result.get("skipped", ())
            } or {((), "confirmed_exit")}
        else:
            return result
        seen = self.failures.setdefault(tool, set())
        repeated = not seen.isdisjoint(signatures)
        seen.update(signatures)
        if not repeated:
            return result
        self.failed_tool = tool
        return {
            **result,
            "next_step": "Finish the report with available evidence. Mark unresolved findings needs_human.",
        }

    def fallback_report(self, closing_reason: str) -> dict:
        """
            生成保守的人工复核报告，供模型无法在收尾轮交付有效报告时使用。

            Returns:
                dict: 已通过业务结构校验的 needs_human 报告。
        """
        if closing_reason == "model_call_budget":
            reason = "调查已达到模型请求上限，收尾轮未提交有效报告。请复核已有观测和待查项。"
        elif closing_reason == "no_progress":
            reason = "连续工具批次未获得新线索，调查已用现有证据收尾。请补充现场观测。"
        elif closing_reason == "context_window":
            reason = "请求超出工作上下文窗口，调查已用现有证据收尾。请缩小排查范围。"
        else:
            reason = f"{closing_reason} 反复返回同类错误或跳过结果，调查已用现有证据收尾。"
        return DiagnosticReport.model_validate({
            "status": "needs_human",
            "summary": reason,
            "recommended_action": {
                "kind": "manual_review", "reason": reason,
                "evidence_ids": list(dict.fromkeys(self.evidence_ids)),
            },
            "unresolved_questions": ["根据现有观测人工复核仍未验证的故障假设。"],
        }).model_dump(mode="json")
