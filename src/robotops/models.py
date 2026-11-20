"""
    API、工具和诊断报告共用的数据契约；边界输入拒绝未知字段与非有限数值。
"""

from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# 限定 1–64 位 ASCII 标识：首位小写字母，后续仅字母/数字/下划线/短横线。
# 首尾锚点禁止把路径片段混成应用/组件名；标识校验不能替代运行归属和动作授权。
Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]


def utc_now() -> str:
    """
        生成用于事件与审计关联的 UTC 墙上时钟时间；不用于计算采样耗时。

        Args:
            无显式参数。

        Returns:
            str: 带 UTC 时区偏移的 ISO 8601 时间字符串。
    """
    return datetime.now(timezone.utc).isoformat()


class Model(BaseModel):
    """
        各边界模型共用严格字段策略：拒绝未知字段，禁止 NaN/Infinity 混入观测。
    """

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class HealthContract(Model):
    """
        应用声明的健康阈值：输入频率单位 Hz，允许的输出静默时间单位秒。

        G0 将阈值提供给调查工具与模型，尚未据此实现常驻自动检测器。
    """

    # 输入不足与输出停滞是两类现象，阈值分别声明，供调查结合依赖关系判断。
    min_input_hz: float = Field(default=5, gt=0)
    max_output_silence_seconds: float = Field(default=1, gt=0)


class ApplicationContract(Model):
    """
        应用显式声明自己的组件；平台不补充示例组件或限制为特定进程名称。
    """

    # app_id 是长期应用身份；version 用于隔离不同版本的历史经验，不代表进程启动次数。
    app_id: Identifier
    version: str = Field(default="1", min_length=1, max_length=64)
    description: str = Field(default="", max_length=1000)
    # 依赖方向为“组件 -> 它需要的上游组件”；未列依赖的组件按无上游处理。
    components: list[Identifier]
    dependencies: dict[Identifier, list[Identifier]] = Field(default_factory=dict)
    health: HealthContract = Field(default_factory=HealthContract)

    @model_validator(mode="after")
    def unique_components(self):
        """
            校验组件集合与有向无环依赖，避免无意义的故障传播范围。

            Args:
                无显式参数。

            Returns:
                ApplicationContract: 通过组件与依赖关系校验的当前模型实例。
        """
        if not self.components or len(set(self.components)) != len(self.components):
            raise ValueError("Components must be nonempty and unique")
        # 先保证所有依赖均在契约内且每组不重复，后面的图遍历才有明确的节点范围。
        known = set(self.components)
        if not set(self.dependencies) <= known or any(
            not set(values) <= known or len(set(values)) != len(values)
            for values in self.dependencies.values()
        ):
            raise ValueError("Dependencies must reference unique declared components")

        # 契约明确使用有向无环依赖；环会让故障上游范围失去清晰语义，注册时直接拒绝。
        checked = set()

        def visit(node, path):
            """
                深度优先检查单个组件的上游依赖。

                Args:
                    node: 当前待访问组件名称。
                    path: 当前递归路径上的组件集合；与全局已完成缓存区分以识别回边。

                Returns:
                    None: 校验可达上游，并把完成检查的节点加入闭包中的缓存集合。
            """
            # 当前路径检测回边；不能只看全局 checked，否则会把环误认成已完成节点。
            if node in path:
                raise ValueError("Application dependencies contain a cycle")
            # 已完整检查的共享上游可复用结论，避免菱形依赖被反复递归展开。
            if node in checked:
                return
            for upstream in self.dependencies.get(node, []):
                # 给各分支独立的路径集合，避免兄弟分支相互污染而误报依赖环。
                visit(upstream, path | {node})
            # 只有所有上游检查完成后才缓存，区分“正在访问”与“已经完成”。
            checked.add(node)

        for component in self.components:
            visit(component, set())
        return self

    def dependency_scope(self, focus: list[str]) -> list[str]:
        """
            返回焦点及其传递上游；未声明焦点时保留全应用范围，不猜测组件关系。

            Args:
                focus (list[str]): 焦点组件列表；为空时以全部声明组件为起点。调用边界负责验证焦点归属。

            Returns:
                list[str]: 按名称排序的焦点及传递上游集合。
        """
        relevant = set(focus or self.components)
        # 显式工作栈计算传递闭包；新增节点才入栈，避免重复依赖造成重复扩展。
        pending = list(relevant)
        while pending:
            for upstream in self.dependencies.get(pending.pop(), []):
                if upstream not in relevant:
                    relevant.add(upstream)
                    pending.append(upstream)
        return sorted(relevant)


class IncidentCreate(Model):
    """
        事件接入数据；symptom 是待调查现象，source 区分手工与检测器来源。
    """

    app_id: Identifier
    # 症状文本是调查输入，不能直接用作已确认根因或恢复成功的依据。
    symptom: str = Field(min_length=1, max_length=2000)
    source: Literal["manual", "detector"] = "manual"


class Hypothesis(Model):
    """
        确认或排除假设都需要观测引用；证据属于当前运行的检查由 Agent 后端完成。
    """

    cause: str = Field(min_length=1, max_length=2000)
    # unverified 允许保留待查方向；supported/refuted 都要求引用，避免只记录模型判断。
    assessment: Literal["supported", "refuted", "unverified"]
    evidence_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_observation(self):
        """
            检查确定性结论是否具备必要的证据引用，不在此判断自然语言结论真假。

            Args:
                无显式参数。

            Returns:
                Hypothesis: 校验通过的当前假设实例。
        """
        if self.assessment != "unverified" and not self.evidence_ids:
            raise ValueError("Supported/refuted hypotheses require evidence")
        return self


class ActionSuggestion(Model):
    """
        模型给出的下一步建议，不是可直接执行的动作授权或恢复成功回执。

        重启建议必须包含目标和证据；后端还要校验其目标属于当前应用契约。
    """

    kind: Literal["none", "collect_more_evidence", "restart_application", "manual_review"]
    # target 使用契约组件标识，不能把模型给出的字符串解释成 shell 命令或任意 PID。
    target: Identifier | None = None
    reason: str = Field(min_length=1, max_length=2000)
    evidence_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_target(self):
        """
            确保重启建议具有可审查的目标和依据；此校验不会授权或执行动作。

            Args:
                无显式参数。

            Returns:
                ActionSuggestion: 校验通过的当前建议实例。
        """
        if self.kind == "restart_application" and (not self.target or not self.evidence_ids):
            raise ValueError("A restart suggestion requires target and evidence")
        return self


class DiagnosticReport(Model):
    """
        模型最终报告；proposed 表示已有受证据支持的假设，needs_human 表示需升级。

        Schema 只能检查结构与必要引用是否提供，不能证明自然语言根因判断真实正确。
        证据归属由调查后端另行校验，恢复是否有效还需要后续独立功能验证。
    """

    # 报告状态与故障是否恢复分开；proposed 只说明已有受支持的诊断假设。
    status: Literal["proposed", "needs_human"]
    summary: str = Field(min_length=1, max_length=4000)
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    recommended_action: ActionSuggestion
    # 未解问题显式留给后续调查/人工处理，避免总结时把不确定性隐去。
    unresolved_questions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_supported_hypothesis(self):
        """
            阻止无受支持假设的报告进入已提出诊断状态。

            Args:
                无显式参数。

            Returns:
                DiagnosticReport: 校验通过的当前诊断报告。
        """
        if self.status == "proposed" and not any(
            h.assessment == "supported" for h in self.hypotheses
        ):
            raise ValueError("A proposed diagnosis requires a supported hypothesis")
        return self

    def referenced_evidence(self) -> set[str]:
        """
            合并假设与动作建议使用的全部证据 ID，供后端一次性检查引用范围。

            Args:
                无显式参数。

            Returns:
                set[str]: 假设和建议共同引用的去重证据 ID 集合。
        """
        return set(self.recommended_action.evidence_ids).union(
            *(set(h.evidence_ids) for h in self.hypotheses)
        )
