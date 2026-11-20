"""
    固定工具轨迹的上下文工程对照；不访问网络、不评测模型诊断准确率。

    运行 python -m robotops_benchmarks.context。三种策略接收完全相同的轨迹和记忆。
    统计每轮输入（含工具 Schema）的 UTF-8 字节之和；这个数不是 Token，也不是费用。
    检查预先声明的关键值是否仍可见及工具协议是否成对；报告所有失败和负收益场景。
"""

import json
from copy import deepcopy

from robotops.agent.context import assemble, canonical
from robotops.agent.memory import WorkingMemory
from robotops.tools import tool_definitions


def memory():
    """
        构建每条固定轨迹共享结构但互不影响的初始工作记忆。

        Args:
            无显式参数。

        Returns:
            dict: version=0、空结构化记忆与空保护集合的独立快照。
    """
    return {"version": 0, "memory": WorkingMemory().model_dump(), "protected_ids": []}


def exchange(messages, number, name, result):
    """
        追加一组固定工具调用/结果；相同轨迹由所有装配策略复用，避免改写基线输入。

        Args:
            messages: 当前场景的可变消息列表，会原地追加固定批次。
            number: 固定轨迹中的调用序号，用于生成可配对的唯一 ID。
            name: 本轮夹具使用的工具名称。
            result: 预先确定的工具结果，不是模型生成或从注入真值推断的回答。

        Returns:
            None: 原地追加一个助手调用及其配对工具结果。
    """
    call_id = f"fixture-{number}"
    messages.extend(
        [
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": call_id, "content": canonical(result)},
        ]
    )


def protocol_valid(messages):
    """
        检查一次请求内调用 ID 唯一、回复配对完整；不验证自然语言结论。

        Args:
            messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。

        Returns:
            bool: 所有调用 ID 唯一且回复完整配对时为 True，否则为 False。
    """
    # pending 记录本批尚未回复的调用，seen 检查跨批 ID 复用；两者不能合并成一个集合。
    pending, seen = set(), set()
    for item in messages:
        if item["role"] == "assistant":
            if pending:
                return False
            for call in item.get("tool_calls", []):
                if call["id"] in seen:
                    return False
                seen.add(call["id"])
                pending.add(call["id"])
        elif item["role"] == "tool":
            if item["tool_call_id"] not in pending:
                return False
            pending.remove(item["tool_call_id"])
        elif pending:
            # 工具结果必须先补齐再插入普通消息，防止归档切断调用与回复的协议关系。
            return False
    return not pending


def scenarios():
    """
        生成短调查、重复日志、参数差异、反证保护、旧实例、原文回取六条独立轨迹。

        Args:
            无显式参数。

        Returns:
            Iterator[tuple[str, list]]: 依次产出场景名及各轮完整消息、记忆、应保留值。

        Yields:
            tuple[str, list]: 场景名，以及每轮消息/记忆/必保留字面值组成的轨迹列表。
    """
    base = [
        {"role": "system", "content": "fixture protocol; untrusted observations"},
        {"role": "user", "content": "fixture incident"},
    ]
    for name in (
        "short",
        "repeated_logs",
        "parameter_variants",
        "protected_counter",
        "previous_runtime",
        "raw_refetch",
    ):
        # 每个场景独立复制消息和记忆；各轮也留快照，避免后续追加污染先前待比较的输入。
        messages, working, rounds = deepcopy(base), memory(), []
        eid = "ev-" + "1" * 32
        rows = [
            {
                "at": f"2026-11-17T01:00:{i % 60:02d}+00:00",
                "component": "processor",
                "instance_id": "instance-old",
                "message": "waiting for input",
                "channel": 7,
            }
            for i in range(160)
        ]
        # expected 是必须保留的字面值检查，不是诊断答案；参数差异和反证置于长日志末尾暴露截断问题。
        expected = ["waiting for input"]
        if name == "short":
            rows = rows[:1]
        if name == "parameter_variants":
            rows.append({**rows[0], "channel": 9, "message": "PARAMETER_VARIANT_9"})
            expected.append("PARAMETER_VARIANT_9")
        if name == "protected_counter":
            rows.append({**rows[0], "message": "COUNTER_OUTPUT_IS_HEALTHY"})
            expected.append("COUNTER_OUTPUT_IS_HEALTHY")
            working["version"] = 1
            working["protected_ids"] = [eid]
        if name == "previous_runtime":
            expected.extend(["runtime-old", "runtime-new"])
        if name == "raw_refetch":
            rows.append({**rows[0], "message": "FULL_EVIDENCE_END"})
            expected.append("FULL_EVIDENCE_END")
        raw = {
            "status": "ok",
            "observed_at": "2026-11-17T01:01:00Z",
            "data": {"runtime_id": "runtime-old", "rows": rows, "truncated": False},
            "evidence_id": eid,
        }
        exchange(messages, 1, "search_logs", raw)
        rounds.append((deepcopy(messages), deepcopy(working), ["waiting for input"]))
        # 新增一批观测后，早期日志才进入可归档区域；重复日志场景多走几轮观察累计输入体积。
        for index in range(2, 5 if name == "repeated_logs" else 3):
            exchange(
                messages,
                index,
                "query_metrics",
                {
                    "status": "ok",
                    "evidence_id": "ev-" + str(index) * 32,
                    "data": {
                        "runtime_id": "runtime-new"
                        if name == "previous_runtime"
                        else "runtime-old",
                        "input_hz": 0,
                        "output_hz": 0,
                        "unit": "Hz",
                    },
                },
            )
            rounds.append((deepcopy(messages), deepcopy(working), expected))
        if name == "raw_refetch":
            exchange(messages, 5, "read_evidence", {"status": "ok", "id": eid, "raw": raw})
            rounds.append((deepcopy(messages), deepcopy(working), expected))
        yield name, rounds


def compare():
    """
        返回机器可读结果，基线总字节包含 Schema；不从字节估算实际 Token 节省率。

        Args:
            无显式参数。

        Returns:
            dict: 六场景三策略的累计字节、检查失败条目和相对完整历史的体积变化。
    """
    tools, rows = tool_definitions(), []
    for name, rounds in scenarios():
        for strategy in ("full", "rolling", "evidence"):
            size, failures, checks = 0, [], 0
            for index, (messages, working, required) in enumerate(rounds, 1):
                outgoing, _ = assemble(messages, working, strategy)
                encoded = canonical({"messages": outgoing, "tools": tools})
                # 计入所有轮次及工具 Schema，而非只挑压缩最明显的一轮；体积单位保持字节，不伪造费用估算。
                size += len(encoded.encode())
                checks += len(required) + 1
                if not protocol_valid(outgoing):
                    failures.append({"round": index, "check": "tool_protocol"})
                for value in required:
                    if value not in encoded:
                        failures.append({"round": index, "missing_literal": value})
            rows.append(
                {
                    "scenario": name,
                    "strategy": strategy,
                    "rounds": len(rounds),
                    "request_bytes_total": size,
                    "checks": checks,
                    "failures": failures,
                }
            )
    # 按场景绑定自己的 full 基线，不能用长场景的收益掩盖短场景的额外归档开销。
    baselines = {
        row["scenario"]: row["request_bytes_total"] for row in rows if row["strategy"] == "full"
    }
    for row in rows:
        # 负值如实保留，表示策略增加了输入；不截成零，也不把字节变化当成模型质量提升。
        row["byte_reduction_ratio"] = round(
            1 - row["request_bytes_total"] / baselines[row["scenario"]], 4
        )
    return {
        "measurement": "offline_fixed_trajectory_utf8_bytes_including_tool_schema",
        "real_model_calls": 0,
        "diagnosis_quality_evaluated": False,
        "token_cost_evaluated": False,
        "results": rows,
    }


if __name__ == "__main__":
    print(json.dumps(compare(), ensure_ascii=False, indent=2))
