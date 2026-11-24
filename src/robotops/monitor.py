"""
    对已登记的服务执行一轮只读检查，状态和事件由 SQLite 同时提交。
"""

from .models import ServiceRegistration
from .service_manager import ServiceManager
from .storage import Store


def poll_services(
    store: Store, manager: ServiceManager, *, plan_recovery_for: str | None = None,
) -> list[dict]:
    """
        读取当前登记项并逐个采样。外部命令在写事务之外运行。

        Args:
            store: 与 API 共用数据目录的业务库。
            manager: 生产环境使用 systemd，测试可注入固定观测。
            plan_recovery_for: 可在确认意外退出时写入待授权动作的应用 ID。

        Returns:
            list[dict]: 各应用的状态、关联事件及本轮是否新建事件。
    """
    results = []
    for service in store.services():
        app_id = service["app_id"]
        target = ServiceRegistration(unit=service["unit"], scope=service["scope"])
        sample = manager.inspect(target)
        result = store.record_service_observation(
            app_id, sample, plan_recovery=app_id == plan_recovery_for,
        )
        results.append({"app_id": app_id, "unit": service["unit"], **result})
    return results
