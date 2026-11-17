"""将 pipeline 的进程、遥测及日志转换为核心需要的只读观测。"""

import json
import time
from datetime import datetime, timezone

from robotops.observations import ObservationUnavailable

from .runtime import PipelineRuntime


class PipelineObserver:
    """实现 ObservationSource，只将观测方法交给核心，不提供场景控制入口。

    这里封装 pipeline 特有的计数、日志文件名和两组件结构，核心无需知道这些细节。
    runtime 的生命周期由示例应用管理，观测对象只读取它拥有的进程和公开数据。
    """

    app_id = "pipeline"

    def __init__(self, runtime: PipelineRuntime):
        self.runtime = runtime

    def inspect_process(self) -> dict:
        """返回实际进程事实和遥测；不根据最后一份心跳推断进程仍然存活。"""
        return self.runtime.snapshot()

    def query_metrics(self, sample_seconds: float) -> dict:
        """以真实计数和单调时间计算速率；实例变化时拒绝比较两个不同进程的计数。

        输出年龄另以 UTC 时间计算，因此单独标记 wall_clock；没有输出时间时返回
        None。计数来自实际 worker 遥测，不使用注入的故障名称推算结果。
        """
        start = self.runtime.snapshot()
        timer = time.monotonic()
        time.sleep(sample_seconds)
        end = self.runtime.snapshot()
        elapsed = time.monotonic() - timer
        if start["runtime_id"] != end["runtime_id"]:
            raise ObservationUnavailable("Runtime changed during sampling")
        rates, freshness = {}, {}
        for component in ("producer", "processor"):
            before = start["components"][component]["telemetry"]
            after = end["components"][component]["telemetry"]
            if not before or before.get("instance_id") != after.get("instance_id"):
                raise ObservationUnavailable(
                    "Telemetry missing or instance changed during sampling"
                )
            rates[component] = {
                key.replace("_count", "_hz"): (after[key] - before[key]) / elapsed
                for key in ("sent_count", "received_count", "processed_count", "rejected_count")
            }
            stamp = after.get("last_output_at")
            freshness[component] = (
                (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds()
                if stamp
                else None
            )
        return {
            "runtime_id": start["runtime_id"],
            "window_start": start["observed_at"],
            "window_end": end["observed_at"],
            "elapsed_seconds": elapsed,
            "rates": rates,
            "output_age_seconds_wall_clock": freshness,
            "current": end,
        }

    def search_logs(self, keyword: str, limit: int, since: datetime | None) -> dict:
        """合并受管来源，按条件过滤后返回最新记录，保留文件和条数截断说明。"""
        rows, truncated = [], False
        runtime_id, malformed, incomplete_tail = None, 0, False
        for source in ("producer.jsonl", "processor.jsonl"):
            read = self.runtime.public_file(source)
            if runtime_id is not None and runtime_id != read["runtime_id"]:
                raise ObservationUnavailable("Runtime changed while reading logs")
            runtime_id = read["runtime_id"]
            truncated |= read["truncated"]
            malformed += read["malformed_lines"]
            incomplete_tail |= read["incomplete_tail"]
            rows.extend(
                line
                for line in read["rows"]
                if keyword.lower() in json.dumps(line).lower()
                and (since is None or datetime.fromisoformat(line["at"]) >= since)
            )
        rows.sort(key=lambda row: datetime.fromisoformat(row["at"]))
        return {
            "runtime_id": runtime_id,
            "rows": rows[-limit:],
            "truncated": truncated or len(rows) > limit,
            "malformed_lines": malformed,
            "incomplete_tail": incomplete_tail,
        }

    def get_recent_config_changes(self) -> dict:
        """读取示例应用配置的最近 20 条变更，超出范围时明确声明截断。"""
        read = self.runtime.public_file("config_changes.jsonl")
        rows = read.pop("rows")
        return {**read, "changes": rows[-20:], "truncated": read["truncated"] or len(rows) > 20}
