"""API、工具和诊断报告共用的数据契约；边界输入拒绝未知字段与非有限数值。"""

from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Model(BaseModel):
    """各边界模型共用严格字段策略：拒绝未知字段，禁止 NaN/Infinity 混入观测。"""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class HealthContract(Model):
    """应用声明的健康阈值：输入频率单位 Hz，允许的输出静默时间单位秒。

    G0 将阈值提供给调查工具与模型，尚未据此实现常驻自动检测器。
    """

    min_input_hz: float = Field(default=5, gt=0)
    max_output_silence_seconds: float = Field(default=1, gt=0)


class ApplicationContract(Model):
    """应用显式声明自己的组件；平台不补充示例组件或限制为特定进程名称。"""

    app_id: Identifier
    version: str = Field(default="1", min_length=1, max_length=64)
    description: str = Field(default="", max_length=1000)
    components: list[Identifier]
    health: HealthContract = Field(default_factory=HealthContract)

    @model_validator(mode="after")
    def unique_components(self):
        if not self.components or len(set(self.components)) != len(self.components):
            raise ValueError("Components must be nonempty and unique")
        return self


class IncidentCreate(Model):
    """事件接入数据；symptom 是待调查现象，source 区分手工与检测器来源。"""

    app_id: Identifier
    symptom: str = Field(min_length=1, max_length=2000)
    source: Literal["manual", "detector"] = "manual"


class Hypothesis(Model):
    """确认或排除假设都需要观测引用；证据属于当前运行的检查由 Agent 后端完成。"""

    cause: str = Field(min_length=1, max_length=2000)
    assessment: Literal["supported", "refuted", "unverified"]
    evidence_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_observation(self):
        if self.assessment != "unverified" and not self.evidence_ids:
            raise ValueError("Supported/refuted hypotheses require evidence")
        return self


class ActionSuggestion(Model):
    """模型给出的下一步建议，不是可直接执行的动作授权或恢复成功回执。

    重启建议必须包含目标和证据；后端还要校验其目标属于当前应用契约。
    """

    kind: Literal["none", "collect_more_evidence", "restart_application", "manual_review"]
    target: Identifier | None = None
    reason: str = Field(min_length=1, max_length=2000)
    evidence_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_target(self):
        if self.kind == "restart_application" and (not self.target or not self.evidence_ids):
            raise ValueError("A restart suggestion requires target and evidence")
        return self


class DiagnosticReport(Model):
    """模型最终报告；proposed 表示已有受证据支持的假设，needs_human 表示需升级。

    Schema 只能检查结构与必要引用是否提供，不能证明自然语言根因判断真实正确。
    证据归属由调查后端另行校验，恢复是否有效还需要后续独立功能验证。
    """

    status: Literal["proposed", "needs_human"]
    summary: str = Field(min_length=1, max_length=4000)
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    recommended_action: ActionSuggestion
    unresolved_questions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_supported_hypothesis(self):
        if self.status == "proposed" and not any(
            h.assessment == "supported" for h in self.hypotheses
        ):
            raise ValueError("A proposed diagnosis requires a supported hypothesis")
        return self

    def referenced_evidence(self) -> set[str]:
        """合并假设与动作建议使用的全部证据 ID，供后端一次性检查引用范围。"""
        return set(self.recommended_action.evidence_ids).union(
            *(set(h.evidence_ids) for h in self.hypotheses)
        )
