"""
    调查中重复校验失败的收尾规则，已有原始证据仍可用于报告。
"""

from ..models import DiagnosticReport
from ..tools import DIRECT_OBSERVATION_TOOLS

OPTIONAL_TOOLS = {"update_working_memory", "dispatch_investigation"}


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

    def fallback_report(self) -> dict:
        """
            生成保守的人工复核报告，供模型无法在收尾轮交付有效报告时使用。

            Returns:
                dict: 已通过业务结构校验的 needs_human 报告。
        """
        reason = f"{self.failed_tool} 反复返回同类错误或跳过结果，调查已用现有证据收尾。"
        return DiagnosticReport.model_validate({
            "status": "needs_human",
            "summary": reason,
            "recommended_action": {
                "kind": "manual_review", "reason": reason,
                "evidence_ids": list(dict.fromkeys(self.evidence_ids)),
            },
            "unresolved_questions": ["根据现有观测人工复核仍未验证的故障假设。"],
        }).model_dump(mode="json")
