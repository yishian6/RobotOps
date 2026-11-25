"""
    只读诊断工具：服务端绑定事件范围，校验参数，并为实际观测保存原始证据。
"""

import json
from collections.abc import Collection
from datetime import datetime
from typing import Annotated

from pydantic import AwareDatetime, Field, ValidationError

from .agent.memory import EvidenceID, MemoryStore, WorkingMemory
from .models import ApplicationContract, Identifier, Model, utc_now
from .observations import ObservationSource, ObservationUnavailable, window_diagnostic_fields
from .retrieval import CaseLibrary
from .storage import Store


class NoArgs(Model):
    """无参工具仍拒绝额外字段，防止调用者注入自选路径、应用或 PID。"""


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


_LOG_PAGE_LIMIT = 20


class EvidenceArgs(Model):
    """在原证据内读取日志分页，或选择观测数据的顶层字段。"""

    evidence_id: EvidenceID
    offset: int = Field(default=0, ge=0, description="日志页起点，从 0 开始。续读使用 next_offset。")
    limit: int = Field(default=_LOG_PAGE_LIMIT, ge=1, le=50, description="每页最多返回的日志条数。")
    fields: list[Annotated[str, Field(min_length=1, max_length=96)]] = Field(
        default_factory=list, max_length=20,
        description="可选。只选择 JSON 日志行或非日志 data 的顶层字段；日志身份和时间自动保留。",
    )


# 单条超长记录也不能把整页撑到失控。被省略的字段仍保存在原证据中。
_EVIDENCE_VIEW_BYTES = 16 * 1024
_LOG_IDENTITY = {"at", "timestamp", "component", "instance_id", "source", "level"}
_LOG_METADATA = {
    "runtime_id", "source", "process_name", "truncated", "malformed_lines",
    "incomplete_tail", "available", "stream", "captured_at",
}


def _evidence_size(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def _log_rows_key(data: dict) -> str | None:
    return next((key for key in ("rows", "lines", "logs") if isinstance(data.get(key), list)), None)


def _evidence_view(entry: dict, args: EvidenceArgs) -> dict:
    """只改变模型看到的正文，保留原证据元数据和哈希。"""
    raw = entry["raw"]
    data = raw.get("data")
    if not isinstance(data, dict):
        return entry
    log_key = _log_rows_key(data) if entry["tool"] == "search_logs" else None
    if log_key is not None:
        selected, page = _log_page(data, log_key, offset=args.offset, limit=args.limit, fields=args.fields)
        return {
            **entry, "view": "evidence_page", "raw": {**raw, "data": selected}, "page": page,
        }
    if args.fields:
        selected = {key: data[key] for key in args.fields if key in data}
    elif _evidence_size(raw) <= _EVIDENCE_VIEW_BYTES:
        return entry
    else:
        selected = window_diagnostic_fields(entry["tool"], data)
    fields = {}
    remaining = _EVIDENCE_VIEW_BYTES
    for key, value in selected.items():
        size = _evidence_size({key: value})
        if size <= remaining:
            fields[key] = value
            remaining -= size
    return {
        **entry, "view": "evidence_fields",
        "available_fields": [key for key in data if len(key) <= 96][:50],
        "raw": {**raw, "data": fields},
    }


def _log_page(
    data: dict, log_key: str, *, offset: int | None = None,
    limit: int = _LOG_PAGE_LIMIT, fields: Collection[str] = (),
) -> tuple[dict, dict]:
    """Build a contiguous page from an offset, or the latest page for a new search."""
    rows = data[log_key]
    include = set(fields) | _LOG_IDENTITY
    metadata = {
        key: value for key, value in data.items()
        if key in _LOG_METADATA and _evidence_size({key: value}) <= 256
    }
    # Preserve short adapter diagnostics outside the rows without growing the page without bound.
    metadata_left = max(0, 1024 - _evidence_size(metadata))
    for key, value in data.items():
        if key == log_key or key in metadata or isinstance(value, (dict, list)):
            continue
        size = _evidence_size({key: value})
        if size <= min(96, metadata_left):
            metadata[key] = value
            metadata_left -= size
    page, omitted_rows = [], []
    remaining = _EVIDENCE_VIEW_BYTES - _evidence_size({**metadata, log_key: []})
    if offset is None:
        indices = range(len(rows) - 1, max(-1, len(rows) - limit - 1), -1)
    else:
        offset = min(offset, len(rows))
        indices = range(offset, min(len(rows), offset + limit))
    for index in indices:
        original = rows[index]
        row = (
            {key: value for key, value in original.items() if key in include}
            if fields and isinstance(original, dict) else original
        )
        size = _evidence_size(row) + 1
        if size > remaining:
            if page:
                break
            # Preserve the row position and identity; the caller may request a smaller set of fields.
            omitted_rows.append(index)
            row = {key: value for key, value in row.items() if key in _LOG_IDENTITY} if isinstance(row, dict) else None
            if _evidence_size(row) + 1 > remaining:
                row = None
            size = _evidence_size(row) + 1
        page.append(row)
        remaining -= size
    if offset is None:
        page.reverse()
        offset = len(rows) - len(page)
    end = offset + len(page)
    return {**metadata, log_key: page}, {
        "offset": offset, "total": len(rows), "returned": len(page),
        "next_offset": end if end < len(rows) else None, "omitted_rows": omitted_rows,
        **({"fields": list(fields)} if fields else {}),
    }


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
        "采样输入输出速率和新鲜度。in_flight_processing_age_ms 是未完成样本耗时，"
        "last_completed_processing_latency_ms 是上次已完成样本耗时。接收为零时同时核对发送速率和在途耗时。",
    ),
    "search_logs": (
        LogArgs,
        "查受管组件日志。limit 控制保存到证据的条数。大型结果只返回最新一页作为预览。"
        "回看此前的记录时，按证据 ID 用 read_evidence 从 offset=0 分页。"
        "确需查看后续日志时，用上次日志结果的 observed_at 作为 since。",
    ),
    "get_recent_config_changes": (NoArgs, "读取受管应用的配置变更记录和当前配置版本。"),
    "read_evidence": (
        EvidenceArgs,
        "按证据 ID 回取本次调查的原始观测。日志默认每页 20 条，按需用 next_offset 续查。"
        "用 fields 缩小返回字段；省略的超长行位置列在 omitted_rows。"
        "原始指标 processing_age_ms 指当前在途耗时，"
        "processing_latency_ms 指上次已完成样本耗时。",
    ),
    "retrieve_cases": (CaseArgs, "检索适用于当前应用版本/组件的已验证历史经验；不代表当前根因。"),
    "update_working_memory": (
        WorkingMemory,
        "仅在后续还需多轮取证时保存事实、假设和待查问题；支持和反对引用均保护原文。"
        "已经可以交报告时直接提交。成功回执只包含记忆版本和受保护引用。",
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


def invalid_arguments(message: str, error: ValidationError) -> dict:
    """
        给模型提供可修正的字段路径，不回显整份失败输入。

        Args:
            message: 工具使用的简短错误说明。
            error: Pydantic 参数校验错误。

        Returns:
            dict: 工具错误及字段路径、错误类型和原因。
    """
    issues = []
    for item in error.errors(include_input=False, include_url=False):
        path = list(item["loc"])
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
        self.incident = store.incident(store.run_incident_id(run_id))
        self._complete_log_scan: dict | None = None

    def execute(self, name: str, arguments: dict, *, visible_evidence: set[str] | None = None):
        """
            返回统一工具结果，不以“未观测到数据”冒充“观测到正常或零速率”。

            未注册工具或参数不合法返回 error，既不访问环境，也不生成新的观测证据。
            观测成功返回 ok；运行未启动、缺少适配器或读取失败返回 unavailable。
            后两类结果都落为原始证据，read_evidence 则回取原记录而不重复生成证据。

            Args:
                name (str): 注册表中的工具名；未知或禁止的名字以 error 结果返回。
                arguments (dict): 对应工具的 JSON 参数对象；Schema、范围错误转换为 error，不扩大权限。
                visible_evidence: 调用角色当前可读取和写入记忆的证据范围。

            Returns:
                dict: 统一的 ok/error/unavailable 结果；观测结果附证据 ID 和原文引用。
        """
        if name not in TOOL_SPECS:
            return {"status": "error", "error": "Unknown or disallowed tool"}
        if name == "read_evidence" and visible_evidence is not None:
            evidence_id = arguments.get("evidence_id")
            if not isinstance(evidence_id, str) or evidence_id not in visible_evidence:
                return {"status": "error", "error": "Evidence is outside this agent's scope"}
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
                origin = self.store.read_evidence(self.run_id, args.evidence_id)
                return {"status": "ok", **_evidence_view(origin, args)}
            except (KeyError, ValueError) as exc:
                return {"status": "error", "error": str(exc)}
        # 工作记忆是模型注记而非环境观测，不为更新记忆本身生成能支撑根因的证据。
        if name == "update_working_memory":
            try:
                memory = MemoryStore(self.store, self.run_id)
                saved = memory.update(args, visible_evidence=visible_evidence)
                return {
                    "status": "ok", "version": saved["version"],
                    "protected_ids": saved["protected_ids"],
                }
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
        cached = self._complete_log_scan
        if (
            name == "search_logs" and cached is not None
            and (visible_evidence is None or cached["evidence_id"] in visible_evidence)
            and (args.since is None or args.since < datetime.fromisoformat(cached["observed_at"]))
        ):
            return {
                **cached, "view": "previous_complete_log_scan",
                "note": "此前完整日志已覆盖当时的数据。本次未重新读取。"
                "通过 read_evidence 查看原文。确需后续日志时用该 observed_at 作为 since。",
            }
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
        reference = {"evidence_id": evidence_id, "raw_uri": f"evidence:{evidence_id}"}
        if (
            name == "search_logs" and raw["status"] == "ok"
            and args.since is None and not args.keyword
            and data.get("truncated") is False
            and not data.get("malformed_lines") and not data.get("incomplete_tail")
        ):
            self._complete_log_scan = {"status": "ok", "observed_at": observed_at, **reference}
        result = {**raw, **reference}
        if name == "search_logs" and raw["status"] == "ok":
            log_key = _log_rows_key(data)
            if log_key and (len(data[log_key]) > _LOG_PAGE_LIMIT or _evidence_size(data) > _EVIDENCE_VIEW_BYTES):
                preview, page = _log_page(data, log_key)
                return {**result, "data": preview, "view": "log_preview", "page": page}
        return result

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
