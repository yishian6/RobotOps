"""
    只读调查的分派与合并。子任务共用证据存储，各自持有工具范围、历史和引用权限。
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from typing import Literal, get_args
from uuid import uuid4

from pydantic import Field, ValidationError

from ..models import DiagnosticReport, Model
from ..observations import compact_model_observation
from ..tools import TOOL_SPECS, tool_definitions
from .client import ModelError
from .context import canonical
from .memory import MemoryStore, WorkingMemory

InvestigationMode = Literal["single", "supervised", "adaptive"]
INVESTIGATION_MODES = get_args(InvestigationMode)
DEFAULT_API_MODE: InvestigationMode = "adaptive"
Domain = Literal["application", "system"]
ROLE_PROMPT_VERSION = "supervision/v1"
ADAPTIVE_PROMPT_VERSION = "adaptive/v2"
SUBMIT_FINDING = "submit_finding"
SUBMIT_REPORT = "submit_report"

ROLE_TOOLS = {
    "supervisor": {"get_application_contract", "retrieve_cases", "update_working_memory", "read_evidence"},
    "application": {"query_metrics", "search_logs", "get_recent_config_changes", "read_evidence"},
    "system": {"inspect_process", "read_evidence"},
}
DIRECT_OBSERVATION_TOOLS = {"inspect_process", "query_metrics", "search_logs", "get_recent_config_changes"}


class RolePrompts(Model):
    """
        运行时固定的角色指令和资源指纹。
    """

    common: str = Field(min_length=1, frozen=True)
    supervisor: str = Field(min_length=1, frozen=True)
    application: str = Field(min_length=1, frozen=True)
    system: str = Field(min_length=1, frozen=True)
    sha256: str = Field(frozen=True)
    adaptive_supervisor: str = Field(min_length=1, frozen=True)
    adaptive_child: str = Field(min_length=1, frozen=True)
    adaptive_sha256: str = Field(frozen=True)


def load_role_prompts() -> RolePrompts:
    """
        加载随包发布的角色指令，冻结本次服务所用内容。

        Args:
            无显式参数。

        Returns:
            RolePrompts: 带原始资源 SHA-256 的运行快照。
    """
    root = files("robotops.resources").joinpath("prompts")
    legacy = root.joinpath(ROLE_PROMPT_VERSION, "roles.json").read_bytes()
    adaptive = root.joinpath(ADAPTIVE_PROMPT_VERSION, "roles.json").read_bytes()
    return RolePrompts.model_validate({
        **json.loads(legacy), **json.loads(adaptive),
        "sha256": hashlib.sha256(legacy).hexdigest(),
        "adaptive_sha256": hashlib.sha256(adaptive).hexdigest(),
    })


class Assignment(Model):
    """
        主 Agent 发出的领域问题及需要显式共享的证据引用。
    """

    role: Domain
    question: str = Field(min_length=1, max_length=1024)
    evidence_ids: list[str] = Field(default_factory=list)


class Dispatch(Model):
    """
        一个并行批次，每个领域至多一项，以便主 Agent 对每个结果赋予明确责任。
    """

    assignments: list[Assignment] = Field(min_length=1, max_length=2)
    confirm_exit_of: str | None = Field(
        default=None,
        description="只确认疑似退出故障时，指定应用契约中的组件，仅分派 system。",
    )


class ObservationProbe(Model):
    """
        委派预计取得的新观测。工具参数由与执行入口相同的 Schema 校验。
    """

    tool: Literal["inspect_process", "query_metrics", "search_logs", "get_recent_config_changes"]
    arguments: dict = Field(default_factory=dict)


def probe_key(tool: str, arguments: dict) -> tuple[str, str]:
    """
        用类型化参数标识一项检查，显式默认值与省略参数视为同一项。

        Args:
            tool: 已知的观测工具名。
            arguments: 工具参数。

        Returns:
            tuple[str, str]: 工具名和稳定编码后的完整参数。
    """
    return tool, canonical(TOOL_SPECS[tool][0].model_validate(arguments).model_dump(mode="json"))


class AdaptiveAssignment(Assignment):
    """
        按需委派必须指出已有证据、尚未回答的问题和期望的新观测。
    """

    evidence_ids: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=1024)
    expected_observation: str = Field(min_length=1, max_length=1024)
    probe: ObservationProbe


class AdaptiveDispatch(Dispatch):
    """
        按需入口的分派协议，与历史监督模式的工具定义独立。
    """

    assignments: list[AdaptiveAssignment] = Field(min_length=1, max_length=2)


class Finding(Model):
    """
        领域 Agent 返回的发现。依据和反证分别引用原文，缺测用独立字段描述。
    """

    status: Literal["complete", "blocked"]
    summary: str = Field(min_length=1)
    support_ids: list[str] = Field(default_factory=list)
    contradict_ids: list[str] = Field(default_factory=list)
    missing_observations: list[str] = Field(default_factory=list)


class RoleScope:
    """
        以相同白名单约束声明和执行。分支只能回查自己的观测和显式共享证据。
    """

    def __init__(self, gateway, role: str, evidence_ids=(), *, direct_observation=False):
        """
            建立单个 Agent 的权限视图。

            Args:
                gateway: 已在服务端绑定应用和调查运行的工具网关。
                role: 主协调或领域角色。
                evidence_ids: 父角色已验证并明确共享的证据 ID。
                direct_observation: 是否允许该主 Agent 直接使用本应用的基础观测工具。

            Returns:
                None: 保存角色白名单和独立证据视图。
        """
        self.gateway = gateway
        self.allowed = ROLE_TOOLS[role]
        if direct_observation:
            self.allowed = self.allowed | DIRECT_OBSERVATION_TOOLS
        self.evidence_ids = set(evidence_ids)
        self.probe_evidence: dict[tuple[str, str], str] = {}
        self.tools = tool_definitions(self.allowed)

    def execute(self, name: str, arguments: dict) -> dict:
        """
            校验角色与可见证据后调用已有网关，登记此角色新取得的证据。

            Args:
                name: 本轮请求的工具名。
                arguments: 已解析的工具参数。

            Returns:
                dict: 网关工具结果或明确的范围错误。
        """
        if name not in self.allowed:
            return {"status": "error", "error": "Tool is outside this agent's scope"}
        if name == "read_evidence":
            evidence_id = arguments.get("evidence_id")
            if not isinstance(evidence_id, str) or evidence_id not in self.evidence_ids:
                return {"status": "error", "error": "Evidence is outside this agent's scope"}
        if name == "update_working_memory":
            # 主 Agent 的记忆只纳入实际共享的分支引用，避免借记忆接口绕过证据视图。
            try:
                references = WorkingMemory.model_validate(arguments).references()
            except ValidationError:
                return {"status": "error", "error": "Invalid working memory arguments"}
            if not references <= self.evidence_ids:
                return {"status": "error", "error": "Memory cites evidence outside this agent's scope"}
        result = self.gateway.execute(name, arguments)
        if result.get("evidence_id"):
            self.evidence_ids.add(result["evidence_id"])
            if name in DIRECT_OBSERVATION_TOOLS:
                self.probe_evidence[probe_key(name, arguments)] = result["evidence_id"]
        return result


class Supervision:
    """
        主 Agent 调度已绑定应用内的只读子任务。等待结束后只接收结构化结果及引用。
    """

    def __init__(self, gateway, incident: dict, contract: dict, prompts: RolePrompts, run_graph, *, mode):
        """
            绑定一次运行的协作状态，供主图分派工具使用。

            Args:
                gateway: 当前运行使用的只读工具网关。
                incident: 服务端读取的事件记录。
                contract: 当前应用的运行契约。
                prompts: 启动时固定的角色指令。
                run_graph: 可为每个角色独立运行的 LangGraph 调查循环。
                mode: 监督对照或按需委派模式，决定主 Agent 工具与分派条件。

            Returns:
                None: 组装当前运行的主角色和分派工具。
        """
        self.gateway = gateway
        self.incident = incident
        self.contract = contract
        self.prompts = prompts
        self.run_graph = run_graph
        self.mode = mode
        self.adaptive = mode == "adaptive"
        self.main = RoleScope(gateway, "supervisor", direct_observation=self.adaptive)
        self.latest_process_evidence = None
        self.triaged = False
        self.dispatch_schema = AdaptiveDispatch if self.adaptive else Dispatch
        self.dispatches = set()
        self.delegated_probes: set[tuple[str, str]] = set()
        self.confirmed_exit = None
        self.metadata = {
            "role_prompt_version": ROLE_PROMPT_VERSION,
            "role_prompt_sha256": prompts.sha256,
        }
        if self.adaptive:
            self.metadata.update({
                "adaptive_prompt_version": ADAPTIVE_PROMPT_VERSION,
                "adaptive_prompt_sha256": prompts.adaptive_sha256,
            })
        self.tools = [
            *self.main.tools,
            {
                "type": "function",
                "function": {
                    "name": "dispatch_investigation",
                    "description": "按问题选择应用或系统调查分支，等待本批必要结果后合并。",
                    "parameters": self.dispatch_schema.model_json_schema(),
                },
            },
            {
                "type": "function",
                "function": {
                    "name": SUBMIT_REPORT,
                    "description": "提交合并后的诊断报告，证据仅引用本角色可见的观测。",
                    "parameters": DiagnosticReport.model_json_schema(),
                },
            },
        ]

    def execute(self, name: str, arguments: dict) -> dict:
        """
            执行主 Agent 的查询或启动一次调查批次。

            Args:
                name: 主 Agent 请求的工具名。
                arguments: 工具参数；分派请求携带具体问题和共享引用。

            Returns:
                dict: 普通工具结果、分支发现，或可由主 Agent 修正的参数错误。
        """
        if name != "dispatch_investigation":
            result = self.main.execute(name, arguments)
            if self.adaptive and name in DIRECT_OBSERVATION_TOOLS and result.get("evidence_id"):
                # 缺测也是一次有效初查。后续只有存在其他取证路径时才应分派。
                self.triaged = True
                if name == "inspect_process":
                    self.latest_process_evidence = (
                        result["evidence_id"] if result["status"] == "ok" else None
                    )
            return compact_model_observation(name, result) if self.adaptive else result
        if self.adaptive and not self.triaged:
            return {"status": "error", "error": "Collect application observations before delegation"}
        if self.adaptive and (
            self.gateway.observer is None or self.gateway.observer.app_id != self.incident["app_id"]
        ):
            return {"status": "error", "error": "No observation source for child tasks; report the gap"}
        try:
            dispatch = self.dispatch_schema.model_validate(arguments)
        except ValidationError:
            return {"status": "error", "error": "Invalid assignments; supply the fields in the dispatch schema"}
        if self.confirmed_exit:
            # 系统事实已满足这次定向检查，继续分派不会回答此前选定的问题。
            return {"status": "skipped", "confirmed_exit": self.confirmed_exit}
        assignments = dispatch.assignments
        roles = [item.role for item in assignments]
        target = dispatch.confirm_exit_of
        if target is not None and (target not in self.contract["components"] or roles != ["system"]):
            return {
                "status": "error",
                "error": "Exit confirmation requires one system task for a registered component",
            }
        if len(set(roles)) != len(roles):
            return {"status": "error", "error": "Assign each role at most once in a batch"}
        if self.adaptive:
            questions = [" ".join(item.question.split()).casefold() for item in assignments]
            if not all(questions) or len(set(questions)) != len(questions):
                return {"status": "error", "error": "Give each child a distinct unanswered question"}
            if any(not item.reason.strip() or not item.expected_observation.strip() for item in assignments):
                return {"status": "error", "error": "Explain the delegation and expected observation"}
        if any(not set(item.evidence_ids) <= self.main.evidence_ids for item in assignments):
            return {"status": "error", "error": "Assignment cites evidence outside the supervisor's scope"}
        if self.adaptive and target and self.latest_process_evidence:
            # 使用最新实测判断重复确认。缺测时旧进程状态不再构成跳过依据。
            confirmed = self._find_exit(target, [self.latest_process_evidence])
            if confirmed:
                return {"status": "skipped", "confirmed_exit": confirmed}
        skipped = []
        if self.adaptive:
            probes = set()
            pending = []
            for item in assignments:
                tool = item.probe.tool
                if tool not in ROLE_TOOLS[item.role] or (target is not None and tool != "inspect_process"):
                    return {"status": "error", "error": "The assigned role cannot perform the requested probe"}
                try:
                    key = probe_key(tool, item.probe.arguments)
                except ValidationError:
                    return {"status": "error", "error": "Probe arguments must match the observation schema"}
                # 用实际要做的查询识别重复，改写任务描述或者换引用也不会重复派工。
                already_seen = self.main.probe_evidence.get(key)
                if already_seen or key in self.delegated_probes or key in probes:
                    skipped.append({
                        "role": item.role, "reason": "Probe already checked or delegated",
                        "evidence_id": already_seen,
                    })
                    continue
                pending.append(item)
                probes.add(key)
            if not pending:
                return {"status": "skipped", "skipped": skipped}
            # 同批还有独立检查时继续执行，主 Agent 无需再发一个模型请求重提任务。
            assignments = pending
            roles = [item.role for item in assignments]
            self.delegated_probes.update(probes)
        else:
            signature = (target, tuple(sorted(
                (item.role, item.question.strip(), tuple(sorted(set(item.evidence_ids))))
                for item in assignments
            )))
            if signature in self.dispatches:
                return {"status": "error", "error": "This investigation has already been dispatched"}
            self.dispatches.add(signature)

        batch_id = uuid4().hex
        store, run_id = self.gateway.store, self.gateway.run_id
        baseline = MemoryStore(store, run_id).current()["version"]
        tasks = [(item, f"{batch_id}:{item.role}", baseline) for item in assignments]
        batch = {"batch_id": batch_id, "roles": roles, "base_memory_version": baseline}
        store.trace(run_id, {"type": "dispatch_started", **batch})
        # 单分支直接执行；两分支独立运行，等待两份终态后才回到主模型。
        if len(tasks) == 1:
            results = [self._investigate_branch(*tasks[0])]
        else:
            with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
                futures = [executor.submit(self._investigate_branch, *task) for task in tasks]
                results = [future.result() for future in futures]
        for item in results:
            self.main.evidence_ids.update(item.get("support_ids", []) + item.get("contradict_ids", []))
        if target is not None:
            # 被阻塞仍可附带已取得的实测，父分支共享的旧观测则不能代替本次检查。
            self.confirmed_exit = self._find_exit(target, [
                eid for eid in results[0].get("support_ids", []) if eid not in assignments[0].evidence_ids
            ])
        store.trace(run_id, {"type": "dispatch_finished", **batch})
        reply = {"status": "ok", "findings": results, "confirmed_exit": self.confirmed_exit}
        if skipped:
            reply["skipped"] = skipped
        return reply

    def _find_exit(self, target: str, evidence_ids: list[str]) -> dict | None:
        """
            用已经通过范围校验的系统观测核对退出目标。

            Args:
                target: 已登记组件。
                evidence_ids: 调用方筛过的新观测引用。

            Returns:
                dict | None: 能确认退出的组件与证据，或没有足够证据。
        """
        for evidence_id in evidence_ids:
            evidence = self.gateway.store.read_evidence(self.gateway.run_id, evidence_id)
            raw = evidence["raw"]
            if evidence["tool"] != "inspect_process" or raw["status"] != "ok":
                continue
            data = raw.get("data")
            components = data.get("components") if isinstance(data, dict) else None
            component = components.get(target) if isinstance(components, dict) else None
            process = component.get("process") if isinstance(component, dict) else None
            if isinstance(process, dict) and process.get("alive") is False:
                return {"component": target, "evidence_id": evidence_id}
        return None

    def _investigate_branch(self, assignment: Assignment, task_id: str, baseline: int) -> dict:
        """
            创建独立子图，保存可追溯发现；分支失败时让主 Agent 看见信息缺口。

            Args:
                assignment: 已验证的问题、角色和共享引用。
                task_id: 用于快照与轨迹归属的服务端任务 ID。
                baseline: 父 Agent 在分派时的工作记忆版本，供合并结果审计。

            Returns:
                dict: 已校验发现，或带失败原因的终态。
        """
        role = assignment.role
        tags = {
            "agent_role": role, "task_id": task_id, "base_memory_version": baseline,
            "investigation_mode": self.mode,
        }
        scope = RoleScope(self.gateway, role, assignment.evidence_ids)
        store, run_id = self.gateway.store, self.gateway.run_id
        # 分支只拿契约、事件和任务引用，父分支的整个历史与工作记忆不会进入子图。
        messages = [
            {"role": "system", "content": getattr(self.prompts, role) + "\n" + self.prompts.common},
            {"role": "user", "content": json.dumps({
                "incident": self.incident,
                "contract": self.contract,
                "assignment": assignment.model_dump(),
            }, ensure_ascii=False)},
        ]
        if self.adaptive:
            messages[0]["content"] += "\n" + self.prompts.adaptive_child
        memory = {"version": 0, "memory": WorkingMemory().model_dump(), "protected_ids": []}
        tools = [
            *scope.tools,
            {
                "type": "function",
                "function": {
                    "name": SUBMIT_FINDING,
                    "description": "提交本分支发现。已完成需要实测支持引用，缺测则提交 blocked 与缺失观测。",
                    "parameters": Finding.model_json_schema(),
                },
            },
        ]

        def validate(content: str) -> dict:
            """
                校验发现结构、证据范围及实测依据。

                Args:
                    content: 子模型交付的 JSON 文本。

                Returns:
                    dict: 合法的领域发现。
            """
            try:
                finding = Finding.model_validate_json(content)
            except ValidationError as exc:
                raise ModelError("Invalid domain finding") from exc
            references = set(finding.support_ids + finding.contradict_ids)
            if not references <= scope.evidence_ids:
                raise ModelError("Domain finding cites evidence outside this agent's scope")
            if finding.status == "complete" and not any(
                item["tool"] in scope.allowed - {"read_evidence"} and item["raw"]["status"] == "ok"
                for item in (store.read_evidence(run_id, eid) for eid in finding.support_ids)
            ):
                raise ModelError("A completed finding requires successful domain observation")
            if self.adaptive and finding.status == "complete":
                planned = probe_key(assignment.probe.tool, assignment.probe.arguments)
                evidence_id = scope.probe_evidence.get(planned)
                if evidence_id not in finding.support_ids or store.read_evidence(
                    run_id, evidence_id
                )["raw"]["status"] != "ok":
                    raise ModelError("A completed assignment must cite its newly collected probe")
            if finding.status == "blocked" and not finding.missing_observations:
                raise ModelError("A blocked finding must explain the missing observation")
            return finding.model_dump()

        store.trace(run_id, {"type": "branch_started", **tags, "assignment": assignment.model_dump()})
        try:
            finding = self.run_graph(
                messages, tools, scope.execute, validate, lambda: memory, tags, self.metadata,
                final_tool=SUBMIT_FINDING,
            )
        except Exception as exc:
            # 只转发已整理错误，避免原始供应商响应进入父提示或持久轨迹。
            finding = {"status": "failed", "error": str(exc) if isinstance(exc, ModelError) else type(exc).__name__}
        result = {**tags, **finding}
        store.trace(run_id, {"type": "branch_finished", **result})
        return result
