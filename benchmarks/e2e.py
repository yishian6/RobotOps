"""
    真实本地故障的端到端评测：同一场景对比上下文策略的报告质量、用量与耗时。

    与 benchmarks/context.py 的固定轨迹不同，这里启动真实生产/处理子进程、注入故障，
    再让模型通过真实只读工具调查。provider=scripted 只验证评测管线与评分逻辑，不代表
    模型诊断能力；provider=deepseek 才产生可对外报告的真实模型结果。

    评分由可复核的检查项组成：报告状态、原因关键词、当前观测证据、建议动作边界、
    工具错误。关键词检查不能替代人工评审，结果应连同报告正文与请求快照一起审查。
"""

import argparse
import json
import statistics
import tempfile
import time
from importlib.resources import files
from pathlib import Path

from robotops_examples.pipeline.observer import PipelineObserver
from robotops_examples.pipeline.runtime import PipelineRuntime

from robotops.agent.client import DeepSeekClient, _reported_usage
from robotops.agent.context import ContextSnapshots
from robotops.agent.supervision import INVESTIGATION_MODES, InvestigationMode
from robotops.agent.workflow import investigate
from robotops.config import Config
from robotops.models import ApplicationContract, DiagnosticReport, IncidentCreate, utc_now
from robotops.storage import Store

CHECKS = ("status", "cause", "evidence", "action", "no_tool_errors")
M1_ROUTES = {"processing_stalled": {"application", "system"}, "processor_exit": {"system"}}

# 每个故障的验收条件：期望报告状态、原因关键词、必须引用的观测来源、动作边界。
# 关键词只是可复核的弱检查；真实结果仍需查看报告正文与证据原文。
SCENARIOS = {
    "input_stopped": {
        "symptom": "应用没有持续输出，请调查原因",
        "cause_terms": ("断流", "停止输入", "input", "上游", "producer"),
        "required_tools": ("query_metrics",),
        "forbid_restart": True,
    },
    "processing_stalled": {
        "symptom": "输入正常但应用没有输出，请调查原因",
        "cause_terms": ("停滞", "stall", "处理", "processor"),
        "required_tools": ("query_metrics",),
        "forbid_restart": False,
    },
    "config_mismatch": {
        "symptom": "应用没有有效输出，请调查原因",
        "cause_terms": ("配置", "通道", "config", "channel", "不匹配", "mismatch"),
        "required_tools": ("get_recent_config_changes", "search_logs"),
        "forbid_restart": True,
    },
    "processor_exit": {
        "symptom": "处理进程似乎已经退出，请调查原因",
        "cause_terms": ("退出", "exit", "终止", "crash", "processor"),
        "required_tools": ("inspect_process",),
        "forbid_restart": False,
    },
}


def load_contract() -> ApplicationContract:
    """
        读取随示例安装的固定契约，避免评测依赖当前工作目录。

        Args:
            无显式参数。

        Returns:
            ApplicationContract: 与示例 pipeline 进程一致的组件与依赖声明。
    """
    resource = files("robotops_examples.pipeline").joinpath("contract.json")
    return ApplicationContract.model_validate_json(resource.read_text(encoding="utf-8"))


class ScriptedModel:
    """
        按真实工具结果执行固定调查流程的测试替身，只验证评测管线，不测量模型能力。

        每次都从当前消息读取最新工具结果并累积证据；报告中的原因与动作由观测值决定，
        不使用注入场景名，因此评分失败能暴露观测或网关的问题，而不是回放标准答案。
    """

    def __init__(self):
        """
            创建一次调查专用的固定流程模型。

            Args:
                无显式参数。

            Returns:
                None: 初始化调用计数、观测缓存与证据 ID 集合。
        """
        self.calls = 0
        self.metrics = None
        self.process = None
        self.config = None
        self.logs = None
        self.evidence = {}

    def __call__(self, messages, tools):
        """
            返回下一步工具调用或最终报告，并解析本轮可见的真实工具结果。

            Args:
                messages: 本轮装配后的消息视图；旧证据可能已被压缩，因此只增量读取新结果。
                tools: 当前工具 Schema；测试替身不直接使用，仅保持与模型协议一致。

            Returns:
                dict: 工具调用响应或最终诊断报告，均带空用量以明确不是真实计费数据。
        """
        self.calls += 1
        self._collect(messages)
        if self.calls == 1:
            return self._call("query_metrics", {"sample_seconds": 0.3}, "step-1")
        if self.calls == 2:
            return self._call("inspect_process", {}, "step-2")
        if self.calls == 3:
            return self._call("get_recent_config_changes", {}, "step-3")
        if self.calls == 4:
            return self._call("search_logs", {"keyword": "", "limit": 10}, "step-4")
        return self._report()

    def _collect(self, messages):
        """
            从工具回复中提取观测与证据 ID；按结果字段区分来源，避免依赖调用顺序。

            Args:
                messages: 当前消息视图，可能包含助手调用与工具回复。

            Returns:
                None: 更新本替身的观测缓存与证据集合。
        """
        for item in messages:
            if item.get("role") != "tool":
                continue
            data = json.loads(item["content"])
            evidence_id = data.get("evidence_id")
            if evidence_id:
                self.evidence[evidence_id] = data
            payload = data.get("data")
            if not isinstance(payload, dict):
                continue
            if "rates" in payload:
                self.metrics = payload
            elif "components" in payload:
                self.process = payload
            elif "changes" in payload:
                self.config = payload
            elif "rows" in payload:
                self.logs = payload

    def _call(self, name, arguments, call_id):
        """
            构造一次合法的单工具调用响应。

            Args:
                name: 工具注册名。
                arguments: 该工具的参数对象，编码为 function.arguments 字符串。
                call_id: 本次运行的唯一调用 ID，避免被工作流判为重复。

            Returns:
                dict: 符合调查协议的工具调用响应，usage 为空表示非真实模型调用。
        """
        return {
            "message": {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(arguments)},
                    }
                ],
            },
            "usage": {},
        }

    def _report(self):
        """
            根据真实观测生成诊断报告；故障判断不读取注入模式或测试真值。

            Args:
                无显式参数。

            Returns:
                dict: 含受支持假设、当前证据引用和动作建议的最终报告。
        """
        rates = (self.metrics or {}).get("rates", {})
        producer_sent = rates.get("producer", {}).get("sent_hz", 0)
        processed = rates.get("processor", {}).get("processed_hz", 0)
        rejected = rates.get("processor", {}).get("rejected_hz", 0)
        processor = (self.process or {}).get("components", {}).get("processor", {})
        alive = (processor.get("process") or {}).get("alive", True)
        changes = (self.config or {}).get("changes", [])
        channel = changes[-1].get("after", {}).get("expected_channel") if changes else None

        metrics_id = self._first_id("rates")
        process_id = self._first_id("components")
        config_id = self._first_id("changes")
        logs_id = self._first_id("rows")
        evidence_ids = [
            item for item in (metrics_id, process_id, config_id, logs_id) if item is not None
        ]
        # 报告原因必须能由当前观测解释；没有证据时不伪造 proposed 结论。
        if alive is False:
            cause = "processor 进程退出，输出中断"
            action = {
                "kind": "restart_application",
                "target": "processor",
                "reason": "处理进程已退出，需要重启并复查输出",
                "evidence_ids": [item for item in (process_id, metrics_id) if item],
            }
        elif producer_sent == 0:
            cause = "producer 停止输入，导致下游没有输出"
            action = {
                "kind": "manual_review",
                "reason": "上游停止输入，重启下游不能修复，需要确认上游状态",
                "evidence_ids": [],
            }
        elif channel == "wrong-channel" or rejected > 0:
            cause = "processor 配置通道不匹配，输入被拒绝"
            action = {
                "kind": "manual_review",
                "reason": "配置错误无法通过重启处理进程修复，需要修正配置",
                "evidence_ids": [],
            }
        elif processed == 0 and producer_sent > 0:
            cause = "processor 处理停滞，输入正常但没有输出"
            action = {
                "kind": "restart_application",
                "target": "processor",
                "reason": "输入正常而处理计数停止增长，需要重启并复测",
                "evidence_ids": [item for item in (metrics_id, process_id) if item],
            }
        else:
            cause = "未发现可解释的故障特征，需要继续取证"
            action = {
                "kind": "collect_more_evidence",
                "reason": "当前观测不足以支持结论",
                "evidence_ids": [],
            }
            return {
                "message": {
                    "role": "assistant",
                    "content": json.dumps(
                        {
                            "status": "needs_human",
                            "summary": cause,
                            "hypotheses": [],
                            "recommended_action": action,
                            "unresolved_questions": ["需要更多当前观测"],
                        },
                        ensure_ascii=False,
                    ),
                },
                "usage": {},
            }
        report = {
            "status": "proposed",
            "summary": cause,
            "hypotheses": [
                {
                    "cause": cause,
                    "assessment": "supported",
                    "evidence_ids": evidence_ids,
                }
            ],
            "recommended_action": action,
            "unresolved_questions": [],
        }
        return {
            "message": {"role": "assistant", "content": json.dumps(report, ensure_ascii=False)},
            "usage": {},
        }

    def _first_id(self, key):
        """
            返回包含指定数据字段的第一条证据 ID，供报告引用具体观测。

            Args:
                key: 工具结果 data 中用于识别来源的字段名，如 rates 或 components。

            Returns:
                str | None: 匹配证据 ID；没有该来源时返回 None。
        """
        for evidence_id, data in self.evidence.items():
            payload = data.get("data")
            if isinstance(payload, dict) and key in payload and data.get("status") == "ok":
                return evidence_id
        return None


def score_run(store: Store, run: dict, scenario: dict) -> dict:
    """
        按可复核检查项评价一次调查，不使用注入场景名作为通过条件。

        Args:
            store (Store): 保存本次运行证据与轨迹的存储。
            run (dict): investigate 返回的运行记录，report 已从 JSON 还原为字典。
            scenario (dict): 该故障的期望状态、关键词、必需工具与动作边界。

        Returns:
            dict: 五项布尔检查；任一项失败都不算通过，并保留失败原因供人工复核。
    """
    report = run.get("report")
    events = store.trace(run["id"])
    tool_events = [event for event in events if event["type"] == "tool"]
    checks = {
        "status": bool(report)
        and run.get("state") == "COMPLETED"
        and report.get("status") == "proposed",
        "cause": False,
        "evidence": False,
        "action": False,
        "no_tool_errors": all(event["result"].get("status") != "error" for event in tool_events),
    }
    if not report:
        return checks
    text = " ".join(
        [
            report.get("summary", ""),
            *(item.get("cause", "") for item in report.get("hypotheses", [])),
            report.get("recommended_action", {}).get("reason", ""),
        ]
    ).lower()
    checks["cause"] = any(term.lower() in text for term in scenario["cause_terms"])
    validated = DiagnosticReport.model_validate(report)
    cited = validated.referenced_evidence()
    cited_tools = set()
    for evidence_id in cited:
        try:
            item = store.read_evidence(run["id"], evidence_id)
        except (KeyError, ValueError):
            continue
        if item["raw"].get("status") == "ok":
            cited_tools.add(item["tool"])
    checks["evidence"] = bool(cited_tools & set(scenario["required_tools"]))
    kind = report.get("recommended_action", {}).get("kind")
    # 已知故障至少应给出下一步；断流/配置错误不能被建议重启下游掩盖。
    checks["action"] = kind != "none" and (
        not scenario["forbid_restart"] or kind != "restart_application"
    )
    return checks


def _usage(events) -> tuple[dict, dict]:
    """
        汇总实报的输入、输出和缓存用量，另记每类字段缺失的次数。

        Args:
            events: 当前运行的轨迹事件列表。

        Returns:
            tuple[dict, dict]: 每个字段的已知小计或 None，以及对应缺失次数。
    """
    reports = [_reported_usage(event.get("usage")) for event in events
               if event["type"] in {"model", "model_error"}]
    counts, missing = {}, {}
    for key in ("prompt_tokens", "completion_tokens", "prompt_cache_hit_tokens", "prompt_cache_miss_tokens"):
        values = [item[key] for item in reports if key in item]
        counts[key] = sum(values) if values else None
        missing[key] = len(reports) - len(values)
    return counts, missing


def run_once(
    mode: str, strategy: str, provider: str, config: Config | None,
    investigation_mode: InvestigationMode = "single",
) -> dict:
    """
        启动真实进程、注入指定故障、执行一次调查并评分，退出时清理全部子进程。

        Args:
            mode: SCENARIOS 中的故障模式。
            strategy: full、rolling 或 evidence 上下文策略。
            provider: scripted 或 deepseek；仅 deepseek 产生真实模型调用。
            config (Config | None): deepseek 模式的配置；scripted 模式可为 None。
            investigation_mode: 单 Agent 或层级式调查，配对比较时使用相同故障和策略。

        Returns:
            dict: 单次运行的状态、检查项、Token、耗时及失败信息。
    """
    if provider == "scripted" and investigation_mode != "single":
        raise ValueError("The scripted benchmark only supports the single-agent protocol")
    scenario = SCENARIOS[mode]
    with tempfile.TemporaryDirectory(prefix="robotops-e2e-") as temporary:
        root = Path(temporary)
        store = Store(root / "data")
        contract = load_contract()
        store.register(contract)
        incident = store.create_incident(
            IncidentCreate(app_id=contract.app_id, symptom=scenario["symptom"])
        )
        runtime = PipelineRuntime(root)
        started = time.monotonic()
        try:
            runtime.start()
            runtime.inject(mode)
            # 给真实进程一个控制周期，让注入后的计数与日志进入下一次采样窗口。
            time.sleep(0.5)
            complete = (
                ScriptedModel() if provider == "scripted" else DeepSeekClient(config).complete
            )
            run = investigate(
                store,
                PipelineObserver(runtime),
                incident["id"],
                complete,
                "scripted-model" if provider == "scripted" else config.model,
                context_strategy=strategy,
                mode=investigation_mode,
            )
        finally:
            runtime.stop()
        elapsed = time.monotonic() - started
        events = store.trace(run["id"])
        snapshots = len(ContextSnapshots(store, run["id"]).list())
        role_calls = {}
        for event in events:
            if event["type"] in {"model", "model_error"}:
                role_calls.setdefault(event.get("agent_role", "single"), []).append(event)
        model_calls = sum(len(calls) for calls in role_calls.values())
        usage, missing = _usage(events)
        if snapshots > model_calls:
            # 已保存输入但未收到响应的尝试属于缺失，不能让部分实报小计冒充本次总消耗。
            missing = {key: count + snapshots - model_calls for key, count in missing.items()}
        initial_prompt = next((event for event in events if event["type"] == "prompt"), {})
        branches = [event for event in events if event["type"] == "branch_finished"]
        roles = {event["agent_role"] for event in branches}
        expected_roles = M1_ROUTES.get(mode) if investigation_mode == "supervised" else None
        confirmed_exit = any(
            event["type"] == "tool" and event["tool"] == "dispatch_investigation"
            and event["result"].get("confirmed_exit")
            for event in events
        )
        checks = score_run(store, run, scenario)
        return {
            "scenario": mode,
            "strategy": strategy,
            "investigation_mode": investigation_mode,
            "provider": provider,
            "state": run.get("state"),
            "error": run.get("error"),
            "report_status": (run.get("report") or {}).get("status"),
            # 保留报告正文供人工复核；证据原文仍在临时目录，不随结果 JSON 导出。
            "report": run.get("report"),
            "checks": checks,
            "passed": all(checks.values()),
            "model_calls": model_calls,
            "model_calls_by_role": {role: len(calls) for role, calls in role_calls.items()},
            "reported_usage_by_role": {role: _usage(calls)[0] for role, calls in role_calls.items()},
            "tool_calls": sum(1 for event in events if event["type"] == "tool"),
            "tool_errors": sum(
                1
                for event in events
                if event["type"] == "tool" and event["result"].get("status") == "error"
            ),
            "snapshots": snapshots,
            "role_prompt_sha256": initial_prompt.get("role_prompt_sha256"),
            "adaptive_prompt_sha256": initial_prompt.get("adaptive_prompt_sha256"),
            **usage,
            "missing_usage": missing,
            "branches": [{"role": event["agent_role"], "status": event["status"]} for event in branches],
            "expected_roles": sorted(expected_roles) if expected_roles is not None else None,
            "confirmed_exit": confirmed_exit,
            "routing_passed": (
                roles == expected_roles and (
                    confirmed_exit if mode == "processor_exit"
                    else all(event["status"] == "complete" for event in branches)
                )
                if expected_roles is not None else None
            ),
            "elapsed_seconds": round(elapsed, 2),
        }


def summarize(results: list[dict]) -> list[dict]:
    """
        按“场景 × 策略”聚合通过率、Token 与耗时，并单列每项检查的通过率。

        Args:
            results: run_once 产生的逐次记录列表。

        Returns:
            list[dict]: 每个场景/策略一行的聚合结果，按场景与策略名排序。
    """
    groups = {}
    for row in results:
        groups.setdefault((row["scenario"], row["strategy"], row["investigation_mode"]), []).append(row)
    summary = []
    for (scenario, strategy, investigation_mode), rows in sorted(groups.items()):
        known_tokens = {
            key: [row[key] for row in rows if row[key] is not None and not row["missing_usage"][key]]
            for key in ("prompt_tokens", "completion_tokens")
        }
        summary.append(
            {
                "scenario": scenario,
                "strategy": strategy,
                "investigation_mode": investigation_mode,
                "runs": len(rows),
                "pass_rate": round(sum(row["passed"] for row in rows) / len(rows), 3),
                "checks": {
                    key: round(sum(row["checks"][key] for row in rows) / len(rows), 3)
                    for key in CHECKS
                },
                **{f"mean_{key}": round(statistics.mean(values), 1) if values else None
                   for key, values in known_tokens.items()},
                "mean_elapsed_seconds": round(
                    statistics.mean(row["elapsed_seconds"] for row in rows), 2
                ),
            }
        )
    return summary


def evaluate(
    provider: str, scenarios: list[str], strategies: list[str], repeats: int,
    investigation_modes: tuple[InvestigationMode, ...] = ("single",),
) -> dict:
    """
        执行完整评测矩阵并返回机器可读结果，调用方负责打印或保存。

        Args:
            provider: scripted 或 deepseek。
            scenarios: 要运行的 SCENARIOS 键列表。
            strategies: 上下文策略列表。
            repeats: 每个场景/策略的重复次数；真实模型评测建议至少 2 次。
            investigation_modes: 每个场景使用的调查模式，默认沿用单 Agent 基线。

        Returns:
            dict: 含逐次结果、聚合表、真实调用计数与生成时间的评测报告。
    """
    config = Config.load() if provider == "deepseek" else None
    results = []
    for scenario in scenarios:
        for strategy in strategies:
            for _ in range(repeats):
                # 同一次重复交替运行所选调查模式，减少执行时间和缓存状态的顺序差异。
                for investigation_mode in investigation_modes:
                    results.append(run_once(scenario, strategy, provider, config, investigation_mode))
    return {
        "measurement": "end_to_end_real_pipeline",
        "provider": provider,
        "model": config.model if config else "scripted",
        "prompt_version": config.prompt_version if config else None,
        "generated_at": utc_now(),
        "real_model_calls": sum(row["model_calls"] for row in results)
        if provider == "deepseek"
        else 0,
        "diagnosis_quality_evaluated": provider == "deepseek",
        "results": results,
        "summary": summarize(results),
    }


def format_markdown(result: dict) -> str:
    """
        将聚合结果打印为可直接粘贴到实验记录的 Markdown 表。

        Args:
            result: evaluate 返回的完整结果字典。

        Returns:
            str: 含口径说明与聚合表的 Markdown 文本。
    """
    lines = [
        f"provider: {result['provider']}；真实模型调用: {result['real_model_calls']}；"
        "关键词检查不能替代人工评审。",
        "",
        "| 场景 | 策略 | 调查模式 | 次数 | 通过率 | 输入 Token | 输出 Token | 平均耗时(s) | 检查项 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in result["summary"]:
        checks = " ".join(f"{key}={value:.2f}" for key, value in row["checks"].items())
        prompt, completion = (
            f"{row[key]:.0f}" if row[key] is not None else "未知"
            for key in ("mean_prompt_tokens", "mean_completion_tokens")
        )
        lines.append(
            f"| {row['scenario']} | {row['strategy']} | {row['investigation_mode']} | {row['runs']} | "
            f"{row['pass_rate']:.2f} | {prompt} | {completion} | {row['mean_elapsed_seconds']:.1f} | {checks} |"
        )
    return "\n".join(lines)


def main(argv=None) -> int:
    """
        命令行入口：预览评测矩阵，或执行已选定的调查并输出结果。

        Args:
            argv: 可选的参数列表，默认读取 sys.argv；测试可传入固定参数。

        Returns:
            int: 0 表示预览或评测完成（个别失败仍是数据）；2 表示配置缺失或参数错误。
    """
    parser = argparse.ArgumentParser(description="RobotOps 端到端故障评测")
    parser.add_argument("--provider", choices=("scripted", "deepseek"), default="scripted")
    parser.add_argument("--scenarios", help="all 或逗号分隔的场景名")
    parser.add_argument("--strategies")
    parser.add_argument("--modes", help="逗号分隔的 single,supervised,adaptive 调查模式")
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--plan", action="store_true", help="只预览评测矩阵，不读取模型配置或开始调查")
    parser.add_argument("--output", type=Path, help="可选 JSON 结果路径")
    args = parser.parse_args(argv)
    # 显式选择真实模型后还须给出矩阵范围，防止直接沿用离线自检的全量默认值。
    if args.provider == "deepseek" and not args.plan and any(
        value is None for value in (args.scenarios, args.strategies, args.modes, args.repeats)
    ):
        parser.error("paid runs require --scenarios, --strategies, --modes and --repeats; preview with --plan")
    scenarios = list(SCENARIOS) if args.scenarios in (None, "all") else args.scenarios.split(",")
    strategies = ["evidence", "full"] if args.strategies is None else args.strategies.split(",")
    modes = ("single",) if args.modes is None else tuple(args.modes.split(","))
    repeats = 1 if args.repeats is None else args.repeats
    if not set(scenarios) <= set(SCENARIOS) or len(set(scenarios)) != len(scenarios):
        parser.error("scenarios must be a unique list of known cases, or all")
    if not set(strategies) <= {"full", "rolling", "evidence"} or len(set(strategies)) != len(strategies):
        parser.error("strategies must be a unique list drawn from full, rolling, evidence")
    if not set(modes) <= set(INVESTIGATION_MODES) or len(set(modes)) != len(modes):
        parser.error("modes must be a unique list drawn from single, supervised, adaptive")
    if args.provider == "scripted" and modes != ("single",):
        parser.error("the scripted benchmark only supports single mode")
    if repeats < 1:
        parser.error("repeats must be positive")
    if args.plan:
        # 调查内模型可能多轮调用，矩阵大小不能作为模型请求数或费用上限。
        print(json.dumps({
            "provider": args.provider, "scenarios": scenarios, "strategies": strategies,
            "modes": modes, "repeats": repeats,
            "planned_investigations": len(scenarios) * len(strategies) * len(modes) * repeats,
            "real_model_calls": 0,
        }, ensure_ascii=False, indent=2))
        return 0
    if args.provider == "deepseek" and not Config.load().api_key:
        print("DEEPSEEK_API_KEY is not set; use --provider scripted for a harness self-check")
        return 2
    result = evaluate(args.provider, scenarios, strategies, repeats, modes)
    print(format_markdown(result))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nJSON 已写入 {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
