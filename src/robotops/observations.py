"""核心与具体运行环境之间的只读边界，核心无需知道示例文件或 ROS 对象。"""

from datetime import datetime
from typing import Protocol


class ObservationUnavailable(ValueError):
    """观测源未就绪或数据不可用，不等同于已确认设备故障。"""


class ObservationSource(Protocol):
    """工具需要的最小观测能力，由实际场景实现并在应用组装时传入。

    app_id 限定被观测应用；方法返回可序列化的事实字典，失败抛 ObservationUnavailable
    或 OSError。接口不提供启动、终止或故障注入，避免调查获得控制程序的能力。
    当前只有 pipeline 示例实现，后续 ROS 适配仍需提供并验证真实观测语义。
    """

    app_id: str

    def inspect_process(self) -> dict:
        """读取组件真实进程状态和对应实例遥测。"""
        ...

    def query_metrics(self, sample_seconds: float) -> dict:
        """在指定秒数窗口采集指标，返回窗口、单位与观测值。"""
        ...

    def search_logs(self, keyword: str, limit: int, since: datetime | None) -> dict:
        """返回指定条件下的日志和截断说明；不接受调用方自选文件路径。"""
        ...

    def get_recent_config_changes(self) -> dict:
        """读取受管应用的近期配置变更，不能修改配置。"""
        ...
