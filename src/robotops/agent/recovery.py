"""
    调查中重复校验失败或连续无进展时，已有原始证据仍可用于报告。
"""

import hashlib
import json

from ..models import DiagnosticReport
from ..tools import DIRECT_OBSERVATION_TOOLS

OPTIONAL_TOOLS = {"update_working_memory", "dispatch_investigation"}
MAX_STALLED_ROUNDS = 3


class InvestigationRecovery:
    """
        在一次主调查中记录可修正的错误和已取得的现场证据。
    """

    def __init__(self, initial_evidence_ids: tuple[str, ...] = ()):
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

    def record_progress(self, tool: str, result: dict) -> bool:
        """Count meaningful results once, independently of freshly assigned evidence IDs."""
        if result.get("status") != "ok":
            return False
        if tool == "dispatch_investigation":
            data = sorted({
                evidence_id for finding in result.get("findings", [])
                for key in ("support_ids", "contradict_ids") for evidence_id in finding.get(key, [])
            })
        elif tool == "read_evidence":
            data = result["raw"].get("data")
        else:
            data = result.get("data")
            if tool == "search_logs" and data and not (data.get("rows") or data.get("lines")):
                return False
            if tool == "retrieve_cases" and data and not data.get("cases"):
                return False
            if tool == "get_recent_config_changes" and data and data.get("changes") == []:
                return False
        if not data:
            return False
        # An actual metric resample includes its window and changing in-flight/heartbeat times.
        signature = hashlib.sha256(json.dumps([tool, data], sort_keys=True).encode()).digest()
        if signature in self.observations:
            return False
        self.observations.add(signature)
        return True

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
