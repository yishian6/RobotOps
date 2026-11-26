"""建立只读调查、校验诊断报告并保存运行结果。"""

import re
from datetime import datetime
from functools import partial
from typing import Literal, get_args

from pydantic import ValidationError

from ..config import Config, ContextWindow
from ..models import DiagnosticReport
from ..observations import ObservationSource
from ..storage import Store
from ..tools import DIRECT_OBSERVATION_TOOLS, ToolGateway, tool_definitions
from .client import ModelError
from .context import ContextSnapshots, Strategy
from .graph import DEFAULT_MAX_MODEL_CALLS, InvestigationGraph, ModelCallBudget
from .memory import MemoryStore
from .prompt_loader import InvestigationPrompt, investigation_messages, load_investigation_prompt
from .supervision import RolePrompts, Supervision, load_role_prompts

InvestigationMode = Literal["single", "supervised"]
INVESTIGATION_MODES = get_args(InvestigationMode)
DEFAULT_API_MODE: InvestigationMode = "single"
SUBMIT_REPORT = "submit_report"
_REPORT_INSTRUCTION = (
    "先查能区分故障假设的现场观测。初始消息已有应用契约。历史案例和工作记忆按需使用。"
    "完整日志已经查过时，只为验证新的时间范围或新线索再查。"
    "能说明故障现象、范围和下一步时，跳过记忆更新，直接通过 submit_report 提交。"
    "未证实的机制列为待查。"
)
_FIRST_SAMPLE_STALL = re.compile(
    r"(?:首[个次条]|第一[个次条])(?:处理)?(?:输入|样本|消息)"
    r"[^，。；！？\n]{0,8}(?:卡住|卡死|阻塞|挂起|停滞|未完成)"
    r"|\bfirst (?:input|sample|message)\b[^.!?\n]{0,30}"
    r"\b(?:got stuck|hung|blocked|stalled|never finished)\b",
    re.IGNORECASE,
)


def _prior_completions(observations) -> dict[str, dict[str, int]]:
    """Index cited live telemetry by component and instance, including instances without a stall."""
    completed_by_instance: dict[str, dict[str, int]] = {}
    for observation in observations:
        raw = observation["raw"]
        if observation["tool"] not in {"query_metrics", "inspect_process"} or raw.get("status") != "ok":
            continue
        data = raw.get("data")
        snapshot = data.get("current", data) if isinstance(data, dict) else None
        components = snapshot.get("components") if isinstance(snapshot, dict) else None
        if not isinstance(components, dict):
            continue
        for name, component in components.items():
            telemetry = component.get("telemetry") if isinstance(component, dict) else None
            if not isinstance(telemetry, dict):
                continue
            instance = telemetry.get("instance_id")
            instance = instance if isinstance(instance, str) else ""
            instances = completed_by_instance.setdefault(name, {})
            instances.setdefault(instance, 0)
            count = telemetry.get("processed_count")
            if type(count) is not int or count <= 0:
                continue
            try:
                completed = datetime.fromisoformat(telemetry["last_output_at"])
                started = datetime.fromisoformat(telemetry["processing_started_at"])
                if completed < started:
                    instances[instance] = max(count, instances[instance])
            except (KeyError, TypeError, ValueError):
                continue
    return completed_by_instance


def _mentioned(value: str, clause: str) -> bool:
    return bool(re.search(rf"(?<![a-z0-9_-]){re.escape(value)}(?![a-z0-9_-])", clause, re.IGNORECASE))


def _validate_stall_timing(
    report: DiagnosticReport, observations: dict[str, dict], components: list[str],
) -> None:
    """Validate a first-sample claim against the live observations cited for that claim."""
    claims = [(report.summary, observations.keys())]
    claims.extend(
        (hypothesis.cause, hypothesis.evidence_ids)
        for hypothesis in report.hypotheses if hypothesis.assessment == "supported"
    )
    for claim, references in claims:
        assertions = [
            clause for clause in re.split(r"[。；;！!？?\n]", claim)
            if _FIRST_SAMPLE_STALL.search(clause)
            and not re.search(r"并非|不是|排除|无法判断|不能断言|不确定|是否|\bnot\b|\bwhether\b", clause)
        ]
        if not assertions:
            continue
        observed = _prior_completions(observations[evidence_id] for evidence_id in references)
        for clause in assertions:
            named = [name for name in components if _mentioned(name, clause)]
            candidates = named or [name for name, instances in observed.items() if any(instances.values())]
            if len(candidates) != 1:
                continue
            name, = candidates
            instances = observed.get(name, {})
            named_instances = [instance for instance in instances if instance and _mentioned(instance, clause)]
            known_instances = named_instances or list(instances)
            if len(known_instances) != 1 or not (count := instances[known_instances[0]]):
                continue
            raise ModelError(
                f"{name} 已完成 {count} 个样本，且上次输出早于当前在途样本。"
                "请改为描述在途样本自观测时间起未完成，以及未验证的阻塞点。"
            )


def _validate_report(
    content: str, *,
    store: Store,
    run_id: str,
    components: list[str],
    visible_evidence: set[str] | None,
    required_evidence_id: str | None,
) -> dict:
    """按当前角色可见的证据和应用契约校验报告。"""
    try:
        validated = DiagnosticReport.model_validate_json(content)
    except ValidationError as exc:
        raise ModelError("Model final report does not match the diagnostic schema") from exc
    available = store.evidence_ids(run_id) if visible_evidence is None else visible_evidence
    references = validated.referenced_evidence()
    if not references <= available:
        raise ModelError("Model cited evidence outside this investigation")
    if required_evidence_id is not None and required_evidence_id not in references:
        raise ModelError("The report must cite the original ROS Topic observation")

    # Load each cited source once for both the existing evidence checks and timing checks.
    observations = {eid: store.read_evidence(run_id, eid) for eid in references}
    for hypothesis in validated.hypotheses:
        if hypothesis.assessment == "unverified":
            continue
        if not any(
            item["tool"] not in {"retrieve_cases", "get_application_contract"}
            and item["raw"].get("status") == "ok"
            for item in (observations[eid] for eid in hypothesis.evidence_ids)
        ):
            raise ModelError("A confirmed hypothesis requires current observation evidence")
    target = validated.recommended_action.target
    if target and target not in components:
        raise ModelError("Model suggested a target outside the application contract")
    _validate_stall_timing(validated, observations, components)
    return validated.model_dump(mode="json")


def _with_trigger_evidence(store: Store, run_id: str, incident: dict) -> tuple[dict, str | None]:
    """将触发调查的 ROS 原始观测保存为可引用证据。"""
    fact = incident.get("ros_fact")
    if fact is None:
        return incident, None
    evidence_id = store.save_ros_fact_evidence(run_id, fact)
    return {
        **incident, "ros_fact": {**fact, "evidence_id": evidence_id},
        "investigation_goal": (
            "用现场证据说明 Topic 静默和可能影响。该记录仅证明采集端在给定窗口内"
            "未收到消息，根因依据不足时保留待查。"
        ),
    }, evidence_id


def investigate(
    store: Store,
    observer: ObservationSource | None,
    incident_id: str,
    complete,
    model_name: str,
    prompt: InvestigationPrompt | None = None,
    context_strategy: Strategy = "evidence",
    mode: InvestigationMode = DEFAULT_API_MODE,
    role_prompts: RolePrompts | None = None,
    *,
    branch_timeout_seconds: float = Config.branch_timeout_seconds,
    automatic: bool = False,
    max_model_calls: int = DEFAULT_MAX_MODEL_CALLS,
    context_window: ContextWindow = ContextWindow(),
):
    """运行只读调查并返回持久化记录。

    complete 接收消息和工具定义。自动调查在创建运行时认领任务，之后的异常
    记录为失败。调查完成表示报告已生成，故障是否恢复由独立验收判断。
    """
    incident = store.incident(incident_id)
    contract = store.contract(incident["app_id"])
    if mode not in INVESTIGATION_MODES:
        raise ValueError("Unknown investigation mode")
    if type(max_model_calls) is not int or max_model_calls < 1:
        raise ValueError("Model-call budget must be a positive integer")
    report_instruction = _REPORT_INSTRUCTION
    if mode == "supervised":
        role_prompts = role_prompts or load_role_prompts()
        report_instruction += "\n" + role_prompts.supervisor
    if automatic:
        report_instruction += "\n报告须引用触发调查的 Topic 观测。未观测到根因时如实列出缺少的现场信息。"
    prompt = prompt or load_investigation_prompt()
    run_id = store.start_run(incident_id, model_name, automatic=automatic)

    try:
        gateway = ToolGateway(store, observer, run_id)
        memory = MemoryStore(store, run_id)
        graph = InvestigationGraph(
            store=store, run_id=run_id, complete=complete, model_name=model_name,
            context_strategy=context_strategy, snapshots=ContextSnapshots(store, run_id),
            budget=ModelCallBudget(max_model_calls), context_window=context_window,
        )
        incident, evidence_id = _with_trigger_evidence(store, run_id, incident)
        initial_evidence = (evidence_id,) if evidence_id is not None else ()
        messages, metadata = investigation_messages(
            incident, contract, prompt, report_tool_instruction=report_instruction,
        )
        report_tool = {
            "type": "function",
            "function": {
                "name": SUBMIT_REPORT,
                "description": "提交诊断报告，只引用本次调查可见的现场证据。",
                "parameters": DiagnosticReport.model_json_schema(),
            },
        }
        if mode == "supervised":
            supervision = Supervision(
                gateway, incident, contract, role_prompts, graph.run,
                branch_timeout_seconds=branch_timeout_seconds,
            )
            tools, execute, role = supervision.tools, supervision.execute, "supervisor"
            visible_evidence = supervision.main.evidence_ids
            visible_evidence.update(initial_evidence)
            metadata = {**metadata, **supervision.metadata}
        else:
            tools, execute, role = tool_definitions(), gateway.execute, "single"
            visible_evidence = None

        # 初始任务已经携带契约。保留显式 v1 基线用于既有的对照运行。
        omitted = set()
        if prompt.version != "investigation/v1":
            omitted.add("get_application_contract")
            if mode == "single" and max_model_calls <= DEFAULT_MAX_MODEL_CALLS:
                omitted.add("update_working_memory")
        if automatic and observer is None:
            omitted.update(DIRECT_OBSERVATION_TOOLS)
        tools = [tool for tool in tools if tool["function"]["name"] not in omitted]
        report = graph.run(
            messages, [*tools, report_tool], execute,
            partial(
                _validate_report, store=store, run_id=run_id, components=contract["components"],
                visible_evidence=visible_evidence, required_evidence_id=evidence_id,
            ),
            memory.current, {"agent_role": role, "investigation_mode": mode}, metadata,
            final_tool=SUBMIT_REPORT, initial_evidence_ids=initial_evidence,
        )
        store.finish_run(run_id, report=report)
    except Exception as exc:
        error = (
            (str(exc).strip() or type(exc).__name__)
            if isinstance(exc, ModelError)
            else f"Investigation stopped: {type(exc).__name__}"
        )
        store.trace(run_id, {"type": "error", "error": error})
        store.finish_run(run_id, error=error)
    return store.run(run_id)
