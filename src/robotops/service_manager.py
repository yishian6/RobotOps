"""
    只读服务管理接口。systemd 实现查询一个已校验的单元，业务检测在监测器和存储中完成。
"""

import re
import subprocess
from datetime import datetime, timezone
from typing import Protocol

from pydantic import TypeAdapter, ValidationError

from .models import ServiceObservation, ServiceRegistration, ServiceUnit

_PROPERTIES = (
    "Id", "LoadState", "ActiveState", "SubState", "Result", "MainPID",
    "InvocationID", "ExecMainStartTimestampMonotonic", "ExecMainStatus",
)
_EXIT_FAILURES = {"exit-code", "signal", "core-dump", "timeout", "watchdog", "oom-kill"}
_UNIT = TypeAdapter(ServiceUnit)


class ServiceManager(Protocol):
    """
        后端依赖的只读服务接口，允许在离线测试中换成固定观测。
    """

    def inspect(self, target: ServiceRegistration) -> ServiceObservation:
        """
            读取已选服务单元的当前状态。

            Args:
                target: 校验过的单元名称和管理器作用域。

            Returns:
                ServiceObservation: 当前状态及现场字段，无法读取时返回 unknown。
        """
        ...


class SystemdServiceManager:
    """
        通过 systemctl show 查询系统或当前用户的服务管理器。
    """

    def inspect(self, target: ServiceRegistration) -> ServiceObservation:
        """
            执行固定的只读查询，保留系统报告的状态及进程实例。

            Args:
                target: 服务登记参数。单元名称只作为 systemctl 的单一位置参数。

            Returns:
                ServiceObservation: 已解析的观测，或带原因的未知状态。
        """
        # 先记观测开始时间，慢查询无法覆盖后来才开始的更新读数。
        observed_at = datetime.now(timezone.utc)
        command = ["systemctl", f"--{target.scope}", "show", "--no-pager"]
        command += [f"--property={','.join(_PROPERTIES)}", "--", target.unit]
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, errors="replace", timeout=5, check=False,
            )
        except subprocess.TimeoutExpired:
            return ServiceObservation(observed_at=observed_at, state="unknown", detail="systemctl 查询超时")
        except OSError:
            return ServiceObservation(observed_at=observed_at, state="unknown", detail="无法启动 systemctl")

        fields = {}
        for line in completed.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator and key in _PROPERTIES:
                fields[key] = value
        load_state = fields.get("LoadState")
        if completed.returncode or load_state != "loaded":
            detail = "服务单元未加载" if load_state and load_state != "loaded" else "无法读取 systemd 服务状态"
            return ServiceObservation(observed_at=observed_at, state="unknown", detail=detail)

        active, sub_state, result = (fields.get(key) or None for key in ("ActiveState", "SubState", "Result"))
        pid = fields.get("MainPID", "")
        main_pid = int(pid) if re.fullmatch(r"[0-9]{1,10}", pid) else 0
        raw_status = fields.get("ExecMainStatus", "")
        exec_main_status = int(raw_status) if re.fullmatch(r"[0-9]{1,10}", raw_status) else None
        invocation = fields.get("InvocationID", "")
        instance_id = invocation if re.fullmatch(r"[0-9a-fA-F]{32}", invocation) and invocation != "0" * 32 else None
        start = fields.get("ExecMainStartTimestampMonotonic", "")
        if instance_id is None and main_pid and re.fullmatch(r"[1-9][0-9]{0,19}", start):
            # PID 会复用，备用身份也必须含实际进程启动标记。
            instance_id = f"{main_pid}:{start}"
        try:
            unit = _UNIT.validate_python(fields.get("Id"))
        except ValidationError:
            return ServiceObservation(observed_at=observed_at, state="unknown", detail="systemd 未返回有效服务标识")

        if active == "active" and sub_state == "running" and main_pid:
            state = "running"
        elif result in _EXIT_FAILURES and (active == "failed" or sub_state == "auto-restart"):
            # systemd 自带重试时也可能观察到退出；需要明确的失败结果才能建事件。
            state = "failed"
        elif (active == "inactive" and result == "success") or (active == "active" and sub_state == "exited"):
            state = "stopped"
        elif active in {"activating", "deactivating", "reloading", "refreshing"}:
            state = "transitioning"
        else:
            state = "unknown"
        return ServiceObservation(
            observed_at=observed_at, state=state, unit=unit, active_state=active, sub_state=sub_state,
            result=result, main_pid=main_pid or None, instance_id=instance_id,
            exec_main_status=exec_main_status,
            detail="服务状态不足以判断运行或退出" if state == "unknown" else None,
        )
