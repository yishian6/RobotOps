"""
    核心与具体运行环境之间的只读边界，核心无需知道示例文件或 ROS 对象。
"""

from datetime import datetime
from typing import Protocol

# 上下文按显式组件名和实例 ID 标记旧证据。两者必须成对保留，不能只依赖外层字典键。
_INSTANCE_FIELDS = {"component", "instance_id", "started_at", "heartbeat_at", "config_version"}
_PROCESS_ID_FIELDS = {"pid", "alive", "state", "start_ticks"}
_METRIC_FIELDS = _INSTANCE_FIELDS | {
    "sent_count", "received_count", "processed_count", "rejected_count",
    "last_input_at", "last_output_at", "processing_started_at", "processing_age_ms",
    "processing_latency_ms", "end_to_end_latency_ms",
}


def compact_model_observation(tool: str, result: dict) -> dict:
    """
        为主 Agent 投影常规进程和指标结果。保留时间、实例和诊断字段，完整观测仍按引用可读。

        Args:
            tool: 已成功执行的观测工具名称。
            result: 网关返回的结果及已持久化的原始证据引用。

        Returns:
            dict: 带视图标记的精简结果，或不符合已知结构的原结果。
    """
    data = result.get("data")
    if result.get("status") != "ok" or not result.get("evidence_id") or not isinstance(data, dict):
        return result
    if tool not in {"inspect_process", "query_metrics"}:
        return result
    metrics = tool == "query_metrics"
    snapshot = data.get("current") if metrics else data
    components = snapshot.get("components") if isinstance(snapshot, dict) else None
    if not isinstance(components, dict) or not components:
        return result

    projected = {}
    for name, component in components.items():
        # 只投影已知的进程快照结构。自定义适配器可以继续返回自己的事实字段。
        telemetry = component.get("telemetry") if isinstance(component, dict) else None
        if not isinstance(telemetry, dict) or not isinstance(telemetry.get("instance_id"), str):
            projected[name] = component
            continue
        telemetry_fields = _METRIC_FIELDS if metrics else _INSTANCE_FIELDS
        projected[name] = {
            **component,
            "telemetry": {
                key: value for key, value in telemetry.items() if key in telemetry_fields
            },
        }
        process = component.get("process")
        if metrics and isinstance(process, dict):
            # 指标保留进程身份，详细资源数据从 inspect_process 获取，避免同一轮重复发送。
            projected[name]["process"] = {
                key: value for key, value in process.items() if key in _PROCESS_ID_FIELDS
            }
    if projected == components:
        return result
    view = {**snapshot, "components": projected}
    return {**result, "data": {**data, "current": view} if metrics else view, "view": "selected_fields"}


class ObservationUnavailable(ValueError):
    """
        观测源未就绪或数据不可用，不等同于已确认设备故障。
    """


class ObservationSource(Protocol):
    """
        工具需要的最小观测能力，由实际场景实现并在应用组装时传入。

        app_id 限定被观测应用；方法返回可序列化的事实字典，失败抛 ObservationUnavailable
        或 OSError。接口不提供启动、终止或故障注入，避免调查获得控制程序的能力。
        当前只有 pipeline 示例实现，后续 ROS 适配仍需提供并验证真实观测语义。
    """

    app_id: str

    def inspect_process(self) -> dict:
        """
            读取组件真实进程状态和对应实例遥测。

            Args:
                无显式参数。

            Returns:
                dict: 由适配器提供的组件进程与实例事实。
        """
        ...

    def query_metrics(self, sample_seconds: float) -> dict:
        """
            在指定秒数窗口采集指标，返回窗口、单位与观测值。

            Args:
                sample_seconds (float): 实际采样窗口，单位秒；工具入口负责范围校验，适配器据此等待采样。

            Returns:
                dict: 由实现方返回的采样窗口、单位、实例和指标。
        """
        ...

    def search_logs(self, keyword: str, limit: int, since: datetime | None) -> dict:
        """
            返回指定条件下的日志和截断说明；不接受调用方自选文件路径。

            Args:
                keyword (str): 日志文本过滤词，空字符串表示不按关键词限制；不作为正则或路径执行。
                limit (int): 调用方要求的最大日志条数，工具入口保证其为合法正数。
                since (datetime | None): 带时区的日志起始时间；为 None 时不追加时间过滤条件。

            Returns:
                dict: 由实现方返回的匹配日志、来源与截断/损坏说明。
        """
        ...

    def get_recent_config_changes(self) -> dict:
        """
            读取受管应用的近期配置变更，不能修改配置。

            Args:
                无显式参数。

            Returns:
                dict: 由实现方提供的近期配置变更及当前可观察版本信息。
        """
        ...
