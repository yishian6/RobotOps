"""
    只读诊断工具：服务端绑定事件范围，校验参数，并为实际观测保存原始证据。
"""

from collections.abc import Collection

from pydantic import AwareDatetime, Field, ValidationError

from .agent.memory import MemoryStore, WorkingMemory
from .models import ApplicationContract, Identifier, Model, utc_now
from .observations import ObservationSource, ObservationUnavailable
from .retrieval import CaseLibrary
from .storage import Store


class NoArgs(Model):
    """
        无参工具仍拒绝额外字段，防止调用者注入自选路径、应用或 PID。
    """

    pass


class MetricArgs(Model):
    """
        一次真实采样的时间窗口，单位秒；范围限制只影响观测操作本身。
    """

    # 窗口过短会放大调度抖动，过长则阻塞同步观测；这不是对 Agent 总调用量的配额。
    sample_seconds: float = Field(default=0.3, ge=0.1, le=2)


class LogArgs(Model):
    """
        日志关键词、最多返回条数及带时区的起始时间，不接受任意文件路径。
    """

    keyword: str = Field(default="", max_length=200)
    limit: int = Field(default=30, ge=1, le=200)
    # 强制带时区，避免本地时间与 UTC 混比；None 表示不附加起始时间过滤。
    since: AwareDatetime | None = None


class EvidenceArgs(Model):
    """
        原文回取只接受平台生成的证据 ID，实际所属运行仍由 Store 校验。
    """

    # 严格匹配平台的 ev- 加 32 位十六进制 ID；此处校验外形，存储层另验归属和哈希。
    evidence_id: str = Field(pattern=r"^ev-[0-9a-f]{32}$")


class CaseArgs(Model):
    """
        检索条件只能收窄本次应用范围；版本由服务端契约确定。
    """

    query: str = Field(min_length=1, max_length=2000)
    # 组件只提供检索焦点，应用/版本从事件契约读取，不能由模型指定另一个应用。
    components: list[Identifier] = Field(default_factory=list)
    limit: int = Field(default=5, ge=1, le=20)


# 同一注册表同时生成模型工具定义并约束执行入口，避免“声明允许”和“实际可调”分离。
TOOL_SPECS = {
    "get_application_contract": (NoArgs, "读取本次事件的应用契约、组件和健康标准。"),
    "inspect_process": (NoArgs, "检查受管组件的真实进程状态、启动实例、内存与遥测。"),
    "query_metrics": (
        MetricArgs,
        "实际采样输入/输出速率、处理耗时与新鲜度；区别输入断流和处理停滞。",
    ),
    "search_logs": (LogArgs, "在受管组件日志中按关键词和起始时间查询，返回来源及截断说明。"),
    "get_recent_config_changes": (NoArgs, "读取受管应用的配置变更记录和当前配置版本。"),
    "read_evidence": (EvidenceArgs, "按证据 ID 回取本次调查已经保存的原始观测。"),
    "retrieve_cases": (CaseArgs, "检索适用于当前应用版本/组件的已验证历史经验；不代表当前根因。"),
    "update_working_memory": (
        WorkingMemory,
        "保存本次调查事实/假设/待查问题；支持和反对引用均保护原文。只修改调查记忆。",
    ),
}
DIRECT_OBSERVATION_TOOLS = {"inspect_process", "query_metrics", "search_logs", "get_recent_config_changes"}


def tool_definitions(allowed: Collection[str] | None = None) -> list[dict]:
    """
        从注册表生成模型函数调用定义；参数 Schema 与实际执行校验共用模型。

        Args:
            allowed: 当前角色允许的工具名称。为 None 时返回注册表中全部工具。

        Returns:
            list[dict]: 从同一注册表生成的模型函数定义和参数 Schema。
    """
    # 按注册表顺序下发以保持稳定前缀，且无需为角色不可用的工具构造 Schema。
    return [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": schema.model_json_schema(),
            },
        }
        for name, (schema, description) in TOOL_SPECS.items()
        if allowed is None or name in allowed
    ]


def invalid_arguments(message: str, error: ValidationError, prefix: tuple = ()) -> dict:
    """
        给模型提供可修正的字段路径，不回显整份失败输入。

        Args:
            message: 工具使用的简短错误说明。
            error: Pydantic 参数校验错误。
            prefix: 嵌套校验所属的工具参数路径。

        Returns:
            dict: 工具错误及字段路径、错误类型和原因。
    """
    issues = []
    for item in error.errors(include_input=False, include_url=False):
        path = [*prefix, *item["loc"]]
        kind = item["type"]
        details = item["msg"]
        if kind == "extra_forbidden" and path:
            # 未声明的键也来自模型，避免把任意文本或整份参数回显到下一轮。
            path[-1] = "<unexpected_field>"
        if kind == "hypothesis_evidence_required":
            path.append(item["ctx"]["field"])
        issues.append({"path": path, "code": kind, "message": details})
    return {"status": "error", "error": message, "validation_errors": issues}


class ToolGateway:
    """
        将工具调用绑定到一个持久化运行记录，服务端据此确定应用范围。

        Agent 只能提供工具名及该工具允许的参数，不能自行更换 run_id、应用或 PID。
        演示故障控制属于开发者 API，不在此注册表中，所以模型无法通过工具直接注入故障。
    """

    def __init__(self, store: Store, observer: ObservationSource | None, run_id: str):
        """
            绑定本次调查的工具访问范围，防止模型自行切换应用或运行。

            Args:
                store (Store): 业务存储实例，提供当前应用、运行记录、证据及 SQLite 事务连接。
                observer (ObservationSource | None): 当前应用的只读观测适配器；为 None 时环境观测返回不可用。
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                None: 固定当前运行、事件与观测依赖，不执行环境检查。
        """
        self.store, self.observer, self.run_id = store, observer, run_id
        run = store.run(run_id)
        self.incident = store.incident(run["incident_id"])

    def execute(self, name: str, arguments: dict):
        """
            返回统一工具结果，不以“未观测到数据”冒充“观测到正常或零速率”。

            未注册工具或参数不合法返回 error，既不访问环境，也不生成新的观测证据。
            观测成功返回 ok；运行未启动、缺少适配器或读取失败返回 unavailable。
            后两类结果都落为原始证据，read_evidence 则回取原记录而不重复生成证据。

            Args:
                name (str): 注册表中的工具名；未知或禁止的名字以 error 结果返回。
                arguments (dict): 对应工具的 JSON 参数对象；Schema、范围错误转换为 error，不扩大权限。

            Returns:
                dict: 统一的 ok/error/unavailable 结果；观测结果附证据 ID 和原文引用。
        """
        if name not in TOOL_SPECS:
            return {"status": "error", "error": "Unknown or disallowed tool"}
        # 先完成入口校验再访问环境；拒绝未知字段，模型不能夹带路径、PID 或运行 ID。
        try:
            args = TOOL_SPECS[name][0].model_validate(arguments)
        except ValidationError as exc:
            if name == "update_working_memory":
                return invalid_arguments("Invalid working memory arguments", exc)
            return {"status": "error", "error": "Invalid tool arguments", "details": str(exc)}
        # 原文回取沿用原证据身份，不生成新的观测时间或 ID，避免把旧结果包装成最新实测。
        if name == "read_evidence":
            try:
                return {"status": "ok", **self.store.read_evidence(self.run_id, args.evidence_id)}
            except (KeyError, ValueError) as exc:
                return {"status": "error", "error": str(exc)}
        # 工作记忆是模型注记而非环境观测，不为更新记忆本身生成能支撑根因的证据。
        if name == "update_working_memory":
            try:
                return {"status": "ok", **MemoryStore(self.store, self.run_id).update(args)}
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
        observed_at = utc_now()
        try:
            if name == "get_application_contract":
                data = self.store.contract(self.incident["app_id"])
            elif name == "search_logs" and "execution_exit" in self.incident:
                # 事件绑定到失败的那次启动，后续重启的同名进程不会替换这份现场记录。
                data = self.store.execution_logs(
                    self.incident["id"], args.keyword, args.limit, args.since,
                )
            elif name == "retrieve_cases":
                try:
                    contract = self.store.contract(self.incident["app_id"])
                    # 焦点扩展到显式上游，便于找到依赖故障案例；未给焦点时不凭空缩小范围。
                    focus = (
                        ApplicationContract.model_validate(contract).dependency_scope(
                            args.components
                        )
                        if args.components
                        else []
                    )
                    # 在结果里保留来源类别，后端也按工具类型阻止它单独证明当前实例根因。
                    data = {
                        "kind": "historical_cases",
                        "current_observation": False,
                        "cases": CaseLibrary(self.store).search(
                            contract,
                            args.query,
                            focus,
                            args.limit,
                        ),
                    }
                except ValueError as exc:
                    return {"status": "error", "error": str(exc)}
            else:
                # 适配器必须属于当前事件应用；没有对应观测能力时明确返回 unavailable。
                if self.observer is None or self.incident["app_id"] != self.observer.app_id:
                    raise ObservationUnavailable("No observation adapter for this application")
                data = self._observe(name, args)
            raw = {"status": "ok", "observed_at": observed_at, "data": data}
        except (ObservationUnavailable, OSError) as exc:
            raw = {"status": "unavailable", "observed_at": observed_at, "error": str(exc)}
        # 不可用观测也要保留依据；但证据写入失败不能冒充工具成功，因此不在这里吞掉持久化错误。
        evidence_id = self.store.save_evidence(self.run_id, name, raw)
        return {**raw, "evidence_id": evidence_id, "raw_uri": f"evidence:{evidence_id}"}

    def _observe(self, name, args):
        """
            只读协议分派；文件布局和指标算法由当前观测适配器负责。

            Args:
                name: 已通过网关验证的环境只读工具名。
                args: 与工具匹配的已验证参数模型；调用前已保证 observer 可用。

            Returns:
                dict: 当前观测适配器产生的原始事实字典。
        """
        if name == "inspect_process":
            return self.observer.inspect_process()
        if name == "query_metrics":
            return self.observer.query_metrics(args.sample_seconds)
        if name == "search_logs":
            return self.observer.search_logs(args.keyword, args.limit, args.since)
        if name == "get_recent_config_changes":
            return self.observer.get_recent_config_changes()
        raise ValueError("Unsupported diagnostic tool")
