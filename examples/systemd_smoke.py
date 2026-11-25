"""
    在独立临时数据目录中验收用户服务退出检测和恢复动作记录。
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from uuid import uuid4

from fastapi.testclient import TestClient

from robotops.api import create_app
from robotops.config import Config
from robotops.models import ServiceRegistration
from robotops.service_manager import SystemdServiceManager


def smoke_check() -> dict:
    """
        在真实 systemd 用户管理器上验证服务登记、自动建单及动作去重。

        Returns:
            dict: 测试单元名、故障事件、重复观测次数和待授权动作。
    """
    with TemporaryDirectory(prefix="robotops-systemd-smoke-") as directory:
        root = Path(directory)
        unit = f"robotops-m2-{uuid4().hex}.service"
        trigger = root / "exit"
        target = ServiceRegistration(unit=unit, scope="user")
        manager = SystemdServiceManager()
        config_file = root / "robotops.toml"
        config_file.write_text('data_dir = "data"\n', encoding="utf-8")
        env = {**os.environ, "ROBOTOPS_CONFIG": str(config_file), "ROBOTOPS_DATA_DIR": str(root / "data")}
        env.pop("DEEPSEEK_API_KEY", None)

        def poll() -> dict:
            """
                通过真实命令行进行一次监测，验证与 API 共享同一数据库。

                Returns:
                    dict: 当前测试服务的状态与对应事件。
            """
            process = subprocess.run(
                [
                    sys.executable, "-m", "robotops.cli", "monitor", "--once",
                ],
                env=env, text=True, capture_output=True, timeout=10, check=True,
            )
            only, = json.loads(process.stdout)
            return only

        try:
            # 文件由验收程序触发。退出码来自测试进程本身，避免用 systemctl stop 模拟意外退出。
            program = "from pathlib import Path; from time import sleep; import sys\n"
            program += "while not Path(sys.argv[1]).exists(): sleep(0.05)\nraise SystemExit(17)"
            subprocess.run(
                [
                    "systemd-run", "--user", "--quiet", f"--unit={unit}",
                    "--property=Type=exec", "--property=Restart=no", "--property=RuntimeMaxSec=30s",
                    sys.executable, "-c", program, str(trigger),
                ],
                check=True, timeout=10,
            )
            app = create_app(Config(data_dir=root / "data"), service_manager=manager)
            with TestClient(app) as client:
                registered = client.post("/services", json=target.model_dump()).raise_for_status().json()
                assert registered["observation"]["state"] == "running", registered
                assert poll()["incident_id"] is None
                assert client.get("/incidents").raise_for_status().json() == []

                trigger.touch()
                deadline = monotonic() + 4
                while manager.inspect(target).state != "failed":
                    if monotonic() >= deadline:
                        raise AssertionError("systemd did not report the example's failed exit")
                    sleep(0.05)
                failure = poll()
                assert failure["created"] and failure["incident_id"], failure
                incident_id = failure["incident_id"]
                event_url = f"/incidents/{incident_id}"
                first = client.get(event_url).raise_for_status().json()
                assert "recovery_action" not in first
                repeat = poll()
                assert repeat["incident_id"] == incident_id and not repeat["created"], repeat

                incident = client.get(event_url).raise_for_status().json()
                observation = incident["service_exit"]
                assert observation["occurrences"] == 2 and incident["source"] == "detector", incident
                assert observation["first_observation"]["result"] == "exit-code", observation
                assert observation["first_observation"]["exec_main_status"] == 17, observation
                assert observation["first_instance_id"] == registered["observation"]["instance_id"], observation
                assert "recovery_action" not in incident
                assert manager.inspect(target).state == "failed"
                return {
                    "unit": unit, "incident_id": incident_id, "occurrences": observation["occurrences"],
                }
        finally:
            # 清理限定为本例随机生成的用户服务，避免残留失败单元或进程。
            for action in ("stop", "reset-failed"):
                subprocess.run(
                    ["systemctl", "--user", action, "--", unit],
                    capture_output=True, timeout=5, check=False,
                )


if __name__ == "__main__":
    print(json.dumps(smoke_check(), ensure_ascii=False, indent=2))
