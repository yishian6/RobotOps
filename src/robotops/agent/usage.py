"""
    从实报用量和请求快照还原调查开销，保留无法归属或缺失的记录。
"""

from collections import Counter, defaultdict

TOKEN_FIELDS = ("prompt_tokens", "completion_tokens")
CACHE_FIELDS = ("prompt_cache_hit_tokens", "prompt_cache_miss_tokens")


def _totals(events: list[dict], requests: int, unreported: int) -> dict:
    """
        对已经归入同一角色或整次运行的模型事件累加计数。

        Args:
            events: 成功响应与记录了错误的模型事件。
            requests: 该范围的上下文请求快照数。
            unreported: 已保存输入但尚无可配对模型事件的次数。

        Returns:
            dict: 实报小计和缺失次数。完全未知的缓存计数保留为 None。
    """
    counts = dict.fromkeys((*TOKEN_FIELDS, *CACHE_FIELDS), 0)
    missing = dict.fromkeys(counts, unreported)
    for event in events:
        data = event.get("usage") or {}
        for key in counts:
            value = data.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                counts[key] += value
            else:
                missing[key] += 1
    return {
        "recorded_attempts": len(events),
        "reported_tokens": {key: counts[key] for key in TOKEN_FIELDS},
        "missing_usage": {key: missing[key] for key in TOKEN_FIELDS},
        "reported_cache_tokens": {
            key: counts[key] if missing[key] < len(events) + unreported else None
            for key in CACHE_FIELDS
        },
        "missing_cache_usage": {key: missing[key] for key in CACHE_FIELDS},
        "context_requests": requests,
        "unreported_attempts": unreported,
    }


def summarize_usage(events: list[dict], snapshots: list[dict]) -> dict:
    """
        按快照序号关联模型响应，统计各角色及整次运行的用量。

        Args:
            events: 本次运行的轨迹，可包含分支和工具事件。
            snapshots: 带 sequence 和可选 agent_role 的请求快照索引。

        Returns:
            dict: 与原 API 兼容的汇总及按角色拆分的 by_role 字典。
    """
    model_events = [event for event in events if event["type"] in {"model", "model_error"}]
    roles = {item["sequence"]: item.get("agent_role") or "unknown" for item in snapshots}
    recorded = defaultdict(list)
    matched = set()
    by_sequence = {}
    for event in model_events:
        sequence = event.get("context_sequence")
        if sequence in roles:
            matched.add(sequence)
            by_sequence[sequence] = event
        role = event.get("agent_role") or roles.get(sequence, "unknown")
        recorded[role].append(event)
    # 并行分支可能已有一轮请求但尚无响应，按序号匹配才能把缺失归到正确的角色。
    requests = Counter(roles.values())
    pending = Counter(role for sequence, role in roles.items() if sequence not in matched)
    by_role = {
        role: _totals(recorded[role], requests[role], pending[role])
        for role in sorted(requests.keys() | recorded.keys())
    }
    reservations = [event for event in events if event["type"] == "model_budget"]
    budget = None
    if reservations:
        # Threads can append events in a different order than they claimed slots.
        latest = max(reservations, key=lambda event: event["model_budget"]["claimed"])["model_budget"]
        main_claims = [
            event for event in reservations
            if event["accepted"] and not event["model_budget"]["reserved_for_parent_report"]
        ]
        main_latest = max(main_claims, key=lambda event: event["model_budget"]["claimed"], default={})
        budget = {
            **{key: latest[key] for key in ("limit", "claimed", "remaining")},
            "denied_claims": sum(not event["accepted"] for event in reservations),
            "report_only": main_latest.get("report_only"),
        }
    window_rows = []
    stages = Counter()
    for item in snapshots:
        window = item.get("context_window")
        if not window:
            continue
        applied = window.get("stages")
        if applied is None:
            applied = [] if window["stage"] == "unchanged" else [window["stage"]]
        stages.update(applied)
        event = by_sequence.get(item["sequence"], {})
        usage = event.get("usage") or {}
        actual = usage.get("prompt_tokens")
        if type(actual) is not int or actual < 0:
            actual = None
        estimated = window["after_estimated_tokens"]
        # Legacy manifests counted bytes under a token label. Keep them available for audit.
        legacy = window["estimator"] == "utf8_request_bytes_with_framing"
        window_rows.append({
            "context_sequence": item["sequence"], "estimator": window["estimator"],
            "stages": applied, "provider_retry": window.get("provider_retry", False),
            "before_estimated_tokens": None if legacy else window["before_estimated_tokens"],
            "after_estimated_tokens": None if legacy else estimated,
            "actual_prompt_tokens": actual,
            "estimate_error_tokens": estimated - actual if not legacy and actual is not None else None,
            **{key: usage.get(key) for key in CACHE_FIELDS},
        })
    return {
        **_totals(model_events, len(snapshots), sum(pending.values())),
        "includes_summary_calls": "summary" in by_role,
        "by_role": by_role,
        "model_budget": budget,
        "context_window": {
            "compacted_requests": sum(bool(row["stages"]) for row in window_rows),
            "stages": dict(stages),
            "provider_rejections": sum(event.get("error_kind") == "context_length" for event in model_events),
            "provider_retries": sum(row["provider_retry"] for row in window_rows),
            "requests": window_rows,
        },
    }
