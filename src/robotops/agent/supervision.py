"""
    只读调查的分派与合并。子任务共用证据存储，各自持有工具范围、历史和引用权限。
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, wait
from importlib.resources import files
from time import monotonic
from typing import Literal
from uuid import uuid4

from pydantic import Field, ValidationError

from ..models import Model
from ..tools import invalid_arguments, tool_definitions
from .client import ModelError
from .memory import MemoryStore, WorkingMemory

Domain = Literal["application", "system"]
ROLE_PROMPT_VERSION = "supervision/v1"
SUBMIT_FINDING = "submit_finding"

ROLE_TOOLS = {
    "supervisor": {"get_application_contract", "retrieve_cases", "update_working_memory", "read_evidence"},
    "application": {"query_metrics", "search_logs", "get_recent_config_changes", "read_evidence"},
    "system": {"inspect_process", "read_evidence"},
}


class RolePrompts(Model):
    """
        运行时固定的角色指令和资源指纹。
    """

    common: str = Field(min_length=1, frozen=True)
    supervisor: str = Field(min_length=1, frozen=True)
    application: str = Field(min_length=1, frozen=True)
    system: str = Field(min_length=1, frozen=True)
    sha256: str = Field(frozen=True)


def load_role_prompts() -> RolePrompts:
    """
        加载随包发布的角色指令，冻结本次服务所用内容。

        Args:
            无显式参数。

        Returns:
            RolePrompts: 带原始资源 SHA-256 的运行快照。
    """
    root = files("robotops.resources").joinpath("prompts")
    roles = root.joinpath(ROLE_PROMPT_VERSION, "roles.json").read_bytes()
    return RolePrompts.model_validate({**json.loads(roles), "sha256": hashlib.sha256(roles).hexdigest()})


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

    def __init__(self, gateway, role: str, evidence_ids=()):
        """
            建立单个 Agent 的权限视图。

            Args:
                gateway: 已在服务端绑定应用和调查运行的工具网关。
                role: 主协调或领域角色。
                evidence_ids: 父角色已验证并明确共享的证据 ID。

            Returns:
                None: 保存角色白名单和独立证据视图。
        """
        self.gateway = gateway
        self.allowed = ROLE_TOOLS[role]
        self.evidence_ids = set(evidence_ids)
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
            except ValidationError as exc:
                return invalid_arguments("Invalid working memory arguments", exc)
            if not references <= self.evidence_ids:
                return {"status": "error", "error": "Memory cites evidence outside this agent's scope"}
        result = self.gateway.execute(name, arguments)
        if result.get("evidence_id"):
            self.evidence_ids.add(result["evidence_id"])
        return result


class Supervision:
    """
        主 Agent 调度已绑定应用内的只读子任务。等待结束后只接收结构化结果及引用。
    """

    def __init__(
        self, gateway, incident: dict, contract: dict, prompts: RolePrompts, run_graph, *,
        branch_timeout_seconds: float,
    ):
        """
            绑定一次运行的协作状态，供主图分派工具使用。

            Args:
                gateway: 当前运行使用的只读工具网关。
                incident: 服务端读取的事件记录。
                contract: 当前应用的运行契约。
                prompts: 启动时固定的角色指令。
                run_graph: 可为每个角色独立运行的 LangGraph 调查循环。
                branch_timeout_seconds: 每批子任务的等待截止时长，单位秒。

            Returns:
                None: 组装当前运行的主角色和分派工具。
        """
        self.gateway = gateway
        self.incident = incident
        self.contract = contract
        self.prompts = prompts
        self.run_graph = run_graph
        self.branch_timeout_seconds = branch_timeout_seconds
        self.main = RoleScope(gateway, "supervisor")
        self.dispatches = set()
        self.confirmed_exit = None
        self.metadata = {
            "role_prompt_version": ROLE_PROMPT_VERSION,
            "role_prompt_sha256": prompts.sha256,
        }
        self.tools = [
            *self.main.tools,
            {
                "type": "function",
                "function": {
                    "name": "dispatch_investigation",
                    "description": "选择应用或系统分支取证，等待本批结果后合并。",
                    "parameters": Dispatch.model_json_schema(),
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
            return self.main.execute(name, arguments)
        try:
            dispatch = Dispatch.model_validate(arguments)
        except ValidationError as exc:
            return invalid_arguments("Invalid assignments", exc)
        if self.confirmed_exit:
            # 定向核实已取得退出事实，继续分派无法回答缺测的退出机制。
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
        if any(not set(item.evidence_ids) <= self.main.evidence_ids for item in assignments):
            return {"status": "error", "error": "Assignment cites evidence outside the supervisor's scope"}
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
        deadline = monotonic() + self.branch_timeout_seconds
        batch = {"batch_id": batch_id, "roles": roles, "base_memory_version": baseline}
        store.trace(run_id, {"type": "dispatch_started", **batch})
        tasks = [
            (item, {
                "agent_role": item.role, "task_id": f"{batch_id}:{item.role}",
                "base_memory_version": baseline, "investigation_mode": "supervised",
            })
            for item in assignments
        ]
        executor = ThreadPoolExecutor(max_workers=len(tasks))
        try:
            futures = []
            for item, tags in tasks:
                store.trace(run_id, {"type": "branch_started", **tags, "assignment": item.model_dump()})
                futures.append(executor.submit(self._investigate_branch, item, tags, deadline))
            completed, _ = wait(futures, timeout=max(0, deadline - monotonic()))
            results = []
            for future, (item, tags) in zip(futures, tasks, strict=True):
                finding = future.result() if future in completed else None
                if finding is None:
                    finding = {
                        "status": "blocked", "blocked_reason": "deadline_exceeded",
                        "missing_observations": [item.question],
                    }
                result = {**tags, **finding}
                results.append(result)
                # 终态只由批次提交，迟到的线程结果不能覆盖 blocked 或被主 Agent 引用。
                store.trace(run_id, {"type": "branch_finished", **result})
        finally:
            # with 会等待运行中的线程。这里只取消尚未开始的任务，运行中的图自行检查截止时间。
            executor.shutdown(wait=False, cancel_futures=True)
        for item in results:
            self.main.evidence_ids.update(item.get("support_ids", []) + item.get("contradict_ids", []))
        if target is not None:
            # 被阻塞仍可附带已取得的实测，父分支共享的旧观测则不能代替本次检查。
            self.confirmed_exit = self._find_exit(target, [
                eid for eid in results[0].get("support_ids", []) if eid not in assignments[0].evidence_ids
            ])
        store.trace(run_id, {"type": "dispatch_finished", **batch})
        return {"status": "ok", "findings": results, "confirmed_exit": self.confirmed_exit}

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

    def _investigate_branch(self, assignment: Assignment, tags: dict, deadline: float) -> dict | None:
        """
            创建独立子图，保存可追溯发现；分支失败时让主 Agent 看见信息缺口。

            Args:
                assignment: 已验证的问题、角色和共享引用。
                tags: 用于请求快照与轨迹归属的任务和角色信息。
                deadline: 单调时钟表示的统一截止时间。

            Returns:
                dict | None: 已校验发现、失败终态，或 None 表示到期缺测。
        """
        role = assignment.role
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
            if finding.status == "complete":
                # 共享证据可供阅读和反驳，但不能充当分支新取得的现场观测。
                observations = [
                    store.read_evidence(run_id, eid)
                    for eid in finding.support_ids if eid not in assignment.evidence_ids
                ]
                if not any(
                    item["tool"] in scope.allowed - {"read_evidence"} and item["raw"]["status"] == "ok"
                    for item in observations
                ):
                    raise ModelError("A completed finding requires newly collected domain observation")
            if finding.status == "blocked" and not finding.missing_observations:
                raise ModelError("A blocked finding must explain the missing observation")
            return finding.model_dump()

        try:
            finding = self.run_graph(
                messages, tools, scope.execute, validate, lambda: memory, tags, self.metadata,
                final_tool=SUBMIT_FINDING, deadline=deadline,
            )
        except Exception as exc:
            if monotonic() >= deadline:
                return None
            # 只转发已整理错误，避免原始供应商响应进入父提示或持久轨迹。
            return {"status": "failed", "error": str(exc) if isinstance(exc, ModelError) else type(exc).__name__}
        return finding
