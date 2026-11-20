"""
    将 pipeline 的进程、遥测及日志转换为核心需要的只读观测。
"""

import json
import time
from datetime import datetime, timezone

from robotops.observations import ObservationUnavailable

from .runtime import PipelineRuntime


class PipelineObserver:
    """
        实现 ObservationSource，只将观测方法交给核心，不提供场景控制入口。

        这里封装 pipeline 特有的计数、日志文件名和两组件结构，核心无需知道这些细节。
        runtime 的生命周期由示例应用管理，观测对象只读取它拥有的进程和公开数据。
    """

    app_id = "pipeline"

    def __init__(self, runtime: PipelineRuntime):
        """
            将示例运行时封装为核心可使用的只读观测适配器。

            Args:
                runtime (PipelineRuntime): 由示例应用拥有和管理生命周期的 PipelineRuntime 实例。

            Returns:
                None: 只保存观测依赖，不取得进程控制权或启动组件。
        """
        self.runtime = runtime

    def inspect_process(self) -> dict:
        """
            返回实际进程事实和遥测；不根据最后一份心跳推断进程仍然存活。

            Args:
                无显式参数。

            Returns:
                dict: 由运行时取得的实际进程、启动实例和遥测快照。
        """
        return self.runtime.snapshot()

    def query_metrics(self, sample_seconds: float) -> dict:
        """
            以真实计数和单调时间计算速率；实例变化时拒绝比较两个不同进程的计数。

            输出年龄另以 UTC 时间计算，因此单独标记 wall_clock；没有输出时间时返回
            None。计数来自实际 worker 遥测，不使用注入的故障名称推算结果。

            Args:
                sample_seconds (float): 实际采样窗口，单位秒；工具入口负责范围校验，适配器据此等待采样。

            Returns:
                dict: 含采样时间窗、elapsed_seconds、Hz 速率、输出年龄及结束快照。
        """
        start = self.runtime.snapshot()
        timer = time.monotonic()
        time.sleep(sample_seconds)
        end = self.runtime.snapshot()
        # sleep 可能受调度影响，用实际单调耗时作分母，不能直接除以请求的 sample_seconds。
        elapsed = time.monotonic() - timer
        if start["runtime_id"] != end["runtime_id"]:
            raise ObservationUnavailable("Runtime changed during sampling")
        rates, freshness = {}, {}
        for component in ("producer", "processor"):
            before = start["components"][component]["telemetry"]
            after = end["components"][component]["telemetry"]
            # 计数只在同一启动实例内可相减；进程重启后的计数重置不能被算成异常负速率。
            if not before or before.get("instance_id") != after.get("instance_id"):
                raise ObservationUnavailable(
                    "Telemetry missing or instance changed during sampling"
                )
            # 同一实例的累计计数做差转为 Hz；未使用的计数仍保留零值，避免不同组件返回形状漂移。
            rates[component] = {
                key.replace("_count", "_hz"): (after[key] - before[key]) / elapsed
                for key in ("sent_count", "received_count", "processed_count", "rejected_count")
            }
            stamp = after.get("last_output_at")
            # 没有输出时间意味着未知新鲜度，用 None 保留不确定性，不能伪装为零延迟。
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
        """
            合并受管来源，按条件过滤后返回最新记录，保留文件和条数截断说明。

            Args:
                keyword (str): 日志文本过滤词，空字符串表示不按关键词限制；不作为正则或路径执行。
                limit (int): 最多返回的日志条数；由工具边界保证为正整数。
                since (datetime | None): 带时区的日志起始时间；为 None 时不追加时间过滤条件。

            Returns:
                dict: 时间排序后的最新匹配日志，以及截断、坏行和未完成末行标记。
        """
        rows, truncated = [], False
        # 两来源必须属于同一会话；坏行数累加、截断标记取并集，避免合并后隐藏局部读取缺口。
        runtime_id, malformed, incomplete_tail = None, 0, False
        for source in ("producer.jsonl", "processor.jsonl"):
            read = self.runtime.public_file(source)
            if runtime_id is not None and runtime_id != read["runtime_id"]:
                raise ObservationUnavailable("Runtime changed while reading logs")
            runtime_id = read["runtime_id"]
            truncated |= read["truncated"]
            malformed += read["malformed_lines"]
            incomplete_tail |= read["incomplete_tail"]
            # 先过滤后取最新 limit 条，避免旧的无关行占满返回配额；当前是 JSON 文本包含匹配而非全文分词。
            rows.extend(
                line
                for line in read["rows"]
                if keyword.lower() in json.dumps(line).lower()
                and (since is None or datetime.fromisoformat(line["at"]) >= since)
            )
        # 先按带时区的实际时刻合并排序，再取最新记录；不能直接比较不同时区的时间字符串。
        rows.sort(key=lambda row: datetime.fromisoformat(row["at"]))
        return {
            "runtime_id": runtime_id,
            "rows": rows[-limit:],
            "truncated": truncated or len(rows) > limit,
            "malformed_lines": malformed,
            "incomplete_tail": incomplete_tail,
        }

    def get_recent_config_changes(self) -> dict:
        """
            读取示例应用配置的最近 20 条变更，超出范围时明确声明截断。

            Args:
                无显式参数。

            Returns:
                dict: 最近至多 20 条变更，附会话 ID 和完整性/截断标记。
        """
        read = self.runtime.public_file("config_changes.jsonl")
        rows = read.pop("rows")
        return {**read, "changes": rows[-20:], "truncated": read["truncated"] or len(rows) > 20}
