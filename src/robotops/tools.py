"""只读诊断工具：服务端绑定事件范围，校验参数，并为实际观测保存原始证据。"""

from pydantic import AwareDatetime, Field, ValidationError

from .agent.memory import MemoryStore, WorkingMemory
from .models import ApplicationContract, Identifier, Model, utc_now
from .observations import ObservationSource, ObservationUnavailable
from .retrieval import CaseLibrary
from .storage import Store


class NoArgs(Model):
    """无参工具仍拒绝额外字段，防止调用者注入自选路径、应用或 PID。"""

    pass


class MetricArgs(Model):
    """一次真实采样的时间窗口，单位秒；范围限制只影响观测操作本身。"""

    sample_seconds: float = Field(default=0.3, ge=0.1, le=2)


class LogArgs(Model):
    """日志关键词、最多返回条数及带时区的起始时间，不接受任意文件路径。"""

    keyword: str = Field(default="", max_length=200)
    limit: int = Field(default=30, ge=1, le=200)
    since: AwareDatetime | None = None


class EvidenceArgs(Model):
    """原文回取只接受平台生成的证据 ID，实际所属运行仍由 Store 校验。"""

    evidence_id: str = Field(pattern=r"^ev-[0-9a-f]{32}$")


class CaseArgs(Model):
    """检索条件只能收窄本次应用范围；版本由服务端契约确定。"""

    query: str = Field(min_length=1, max_length=2000)
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


def tool_definitions():
    """从注册表生成模型函数调用定义；参数 Schema 与实际执行校验共用模型。"""
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
    ]


class ToolGateway:
    """将工具调用绑定到一个持久化运行记录，服务端据此确定应用范围。

    Agent 只能提供工具名及该工具允许的参数，不能自行更换 run_id、应用或 PID。
    演示故障控制属于开发者 API，不在此注册表中，所以模型无法通过工具直接注入故障。
    """

    def __init__(self, store: Store, observer: ObservationSource | None, run_id: str):
        self.store, self.observer, self.run_id = store, observer, run_id
        run = store.run(run_id)
        self.incident = store.incident(run["incident_id"])

    def execute(self, name: str, arguments: dict):
        """返回统一工具结果，不以“未观测到数据”冒充“观测到正常或零速率”。

        未注册工具或参数不合法返回 error，既不访问环境，也不生成新的观测证据。
        观测成功返回 ok；运行未启动、缺少适配器或读取失败返回 unavailable。
        后两类结果都落为原始证据，read_evidence 则回取原记录而不重复生成证据。
        """
        if name not in TOOL_SPECS:
            return {"status": "error", "error": "Unknown or disallowed tool"}
        try:
            args = TOOL_SPECS[name][0].model_validate(arguments)
        except ValidationError as exc:
            return {"status": "error", "error": "Invalid tool arguments", "details": str(exc)}
        if name == "read_evidence":
            try:
                return {"status": "ok", **self.store.read_evidence(self.run_id, args.evidence_id)}
            except (KeyError, ValueError) as exc:
                return {"status": "error", "error": str(exc)}
        if name == "update_working_memory":
            try:
                return {"status": "ok", **MemoryStore(self.store, self.run_id).update(args)}
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
        observed_at = utc_now()
        try:
            if name == "get_application_contract":
                data = self.store.contract(self.incident["app_id"])
            elif name == "retrieve_cases":
                try:
                    contract = self.store.contract(self.incident["app_id"])
                    focus = (
                        ApplicationContract.model_validate(contract).dependency_scope(
                            args.components
                        )
                        if args.components
                        else []
                    )
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
                if self.observer is None or self.incident["app_id"] != self.observer.app_id:
                    raise ObservationUnavailable("No observation adapter for this application")
                data = self._observe(name, args)
            raw = {"status": "ok", "observed_at": observed_at, "data": data}
        except (ObservationUnavailable, OSError) as exc:
            raw = {"status": "unavailable", "observed_at": observed_at, "error": str(exc)}
        evidence_id = self.store.save_evidence(self.run_id, name, raw)
        return {**raw, "evidence_id": evidence_id, "raw_uri": f"evidence:{evidence_id}"}

    def _observe(self, name, args):
        """只读协议分派；文件布局和指标算法由当前观测适配器负责。"""
        if name == "inspect_process":
            return self.observer.inspect_process()
        if name == "query_metrics":
            return self.observer.query_metrics(args.sample_seconds)
        if name == "search_logs":
            return self.observer.search_logs(args.keyword, args.limit, args.since)
        if name == "get_recent_config_changes":
            return self.observer.get_recent_config_changes()
        raise ValueError("Unsupported diagnostic tool")
