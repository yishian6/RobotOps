"""
    M2 服务登记、退出检测和只读 systemd 边界的离线验收。
"""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

import pytest
from fastapi.testclient import TestClient

from robotops.api import create_app
from robotops.config import Config
from robotops.models import ServiceObservation, ServiceRegistration
from robotops.monitor import poll_services
from robotops.service_manager import SystemdServiceManager
from robotops.storage import Store

UNIT = "detector.service"
START = datetime(2026, 11, 23, tzinfo=timezone.utc)


def sample(state, offset=0, **fields):
    """
        创建时间固定、可模拟重复或乱序投递的服务读数。

        Args:
            state: 归类后的服务状态。
            offset: 距基准时间的秒数。
            **fields: 覆盖观测中的单元、进程实例及故障细节。

        Returns:
            ServiceObservation: 离线适配器可返回的完整读数。
    """
    return ServiceObservation(
        state=state, observed_at=START + timedelta(seconds=offset),
        **{"unit": UNIT, **fields},
    )


def test_service_registration_and_detection_share_sqlite(tmp_path):
    """
        用相互独立的 API 和轮询存储对象验证登记、缺测和故障去重。

        Args:
            tmp_path: 只用于本例的持久数据目录。

        Returns:
            None: 只在运行基线后的明确失败建单，且持续异常续记原事件。
    """
    manager = Mock(spec=SystemdServiceManager)
    manager.inspect.return_value = sample("running", main_pid=17, instance_id="before")
    worker_store = Store(tmp_path)
    with TestClient(create_app(Config(data_dir=tmp_path), service_manager=manager)) as client:
        registered = client.post("/services", json={
            "unit": "detector-alias.service", "scope": "user", "name": "路面检测",
        })
        assert registered.status_code == 201, registered.text
        service = registered.json()
        app_id = service["app_id"]
        assert service["unit"] == UNIT and service["config_version"] == 1
        assert client.get(f"/applications/{app_id}").json()["components"] == ["service"]
        assert client.post("/services", json={"unit": UNIT, "scope": "user"}).status_code == 409
        assert len(client.get("/services").json()) == 1
        with worker_store.connection() as db:
            assert db.execute("SELECT count(*) FROM applications").fetchone()[0] == 1
        assert client.get("/incidents").json() == []

        manager.inspect.return_value = sample("failed", 0.5, unit="other.service", result="exit-code")
        assert poll_services(worker_store, manager)[0]["state"] == "unknown"
        assert client.get("/incidents").json() == []
        manager.inspect.return_value = sample("unknown", 1, unit=None, detail="查询超时")
        assert not poll_services(worker_store, manager)[0]["created"]
        assert manager.inspect.call_args.args[0].unit == UNIT
        status = client.get(f"/services/{app_id}").json()
        assert status["observation"]["state"] == "unknown"
        assert status["last_known"]["state"] == "running"
        assert client.get("/incidents").json() == []

        manager.inspect.return_value = sample("failed", 2, active_state="failed", result="exit-code")
        result, = poll_services(worker_store, manager)
        assert result["created"] is True
        incident_id = result["incident_id"]
        manager.inspect.return_value = sample("failed", 3, active_state="failed", result="exit-code")
        # 换一个 Store 模拟监测进程重启，确认去重依赖数据库而非进程内状态。
        result, = poll_services(Store(tmp_path), manager)
        assert not result["created"] and result["incident_id"] == incident_id
        assert len(client.get("/incidents").json()) == 1
        incident = client.get(f"/incidents/{incident_id}").json()
        assert incident["source"] == "detector" and incident["state"] == "OPEN"
        assert "recovery_action" not in incident
        exit_record = incident["service_exit"]
        assert exit_record["occurrences"] == 2
        assert exit_record["first_instance_id"] == exit_record["latest_instance_id"] == "before"
        assert exit_record["first_observation"]["observed_at"] < exit_record["latest_observation"]["observed_at"]

        manager.inspect.return_value = sample("running", 4, main_pid=23, instance_id="after")
        poll_services(worker_store, manager)
        assert client.get(f"/incidents/{incident_id}").json()["state"] == "OPEN"
        # 迟到的旧样本不能把新运行实例覆盖成失败，也不能增加旧事件的采样次数。
        manager.inspect.return_value = sample("failed", 3, result="exit-code")
        poll_services(worker_store, manager)
        assert client.get(f"/services/{app_id}").json()["observation"]["instance_id"] == "after"
        assert client.get(f"/incidents/{incident_id}").json()["service_exit"]["occurrences"] == 2


@pytest.mark.parametrize("baseline", ["failed", "stopped", "running"])
def test_service_without_running_baseline_or_after_clean_stop_has_no_incident(tmp_path, baseline):
    """
        验证已有停止、首次就失败和干净停止的服务不会被当作确认意外退出。

        Args:
            tmp_path: 独立业务数据目录。
            baseline: 首次登记所能观测到的服务状态。

        Returns:
            None: 缺少有效运行基线时仅持续记录状态。
    """
    store = Store(tmp_path)
    service = store.register_service(ServiceRegistration(unit=UNIT), sample(baseline))
    app_id = service["app_id"]
    if baseline == "running":
        store.record_service_observation(app_id, sample("stopped", 1, result="success"))
    store.record_service_observation(app_id, sample("failed", 2, result="exit-code"))
    assert store.incidents() == []


def test_concurrent_failed_observations_share_one_incident(tmp_path):
    """
        两次并发探测同一个失败状态时，创建事件和更新观测保持原子一致。

        Args:
            tmp_path: 供多个连接共享的隔离数据库。

        Returns:
            None: 只创建一个事件，同一个读数只计一次。
    """
    store = Store(tmp_path)
    app_id = store.register_service(ServiceRegistration(unit=UNIT), sample("running"))["app_id"]
    failed = sample("failed", 1, result="exit-code")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(store.record_service_observation, app_id, failed) for _ in range(2)]
        assert sum(future.result()["created"] for future in futures) == 1
    incident, = store.incidents()
    assert store.incident(incident["id"])["service_exit"]["occurrences"] == 1


def test_monitor_preserves_legacy_action_without_creating_new_ones(tmp_path):
    store = Store(tmp_path)
    target = store.register_service(
        ServiceRegistration(unit=UNIT, scope="user"), sample("running", instance_id="before"),
    )
    manager = Mock(spec=SystemdServiceManager)
    manager.inspect.return_value = sample("failed", 1, result="exit-code")
    result, = poll_services(store, manager)
    incident_id = result["incident_id"]
    assert result["created"] and "recovery_action" not in store.incident(incident_id)
    with store.connection() as db:
        db.execute(
            "INSERT INTO recovery_actions VALUES "
            "('legacy-action', ?, 'restart_service', 'AWAITING_AUTHORIZATION', ?, "
            "'user', ?, 'before', '{}', '2026-01-01T00:00:00+00:00')",
            (incident_id, UNIT, target["config_version"]),
        )
    manager.inspect.return_value = sample("failed", 2, result="exit-code")
    repeat, = poll_services(store, manager)
    assert not repeat["created"] and repeat["incident_id"] == incident_id
    assert store.incident(incident_id)["recovery_action"]["id"] == "legacy-action"
    assert manager.method_calls == [call.inspect(ServiceRegistration(unit=UNIT, scope="user"))] * 2
    with store.connection() as db:
        assert db.execute("SELECT COUNT(*) FROM recovery_actions").fetchone()[0] == 1


def test_systemd_inspect_is_read_only_and_reports_unavailable(monkeypatch):
    """
        验证固定的 systemctl 参数、故障分类、正确的实例身份及超时返回。

        Args:
            monkeypatch: pytest 提供的可恢复 subprocess 替身。

        Returns:
            None: 查询命令只读，故障与缺测有不同状态。
    """
    shell = Mock(return_value=subprocess.CompletedProcess([], 0, stdout=(
        f"Id={UNIT}\nLoadState=loaded\nActiveState=failed\nSubState=failed\n"
        f"Result=exit-code\nMainPID=0\nInvocationID={'1' * 32}\nExecMainStatus=17\n"
    ), stderr=""))
    monkeypatch.setattr("robotops.service_manager.subprocess.run", shell)
    target = ServiceRegistration(unit=UNIT, scope="user")
    manager = SystemdServiceManager()
    observation = manager.inspect(target)
    assert observation.state == "failed" and observation.instance_id == "1" * 32
    assert observation.exec_main_status == 17
    argv, = shell.call_args.args
    assert argv[:4] == ["systemctl", "--user", "show", "--no-pager"]
    assert argv[-2:] == ["--", UNIT] and shell.call_args.kwargs["timeout"] == 5
    shell.side_effect = subprocess.TimeoutExpired(argv, 5)
    unknown = manager.inspect(target)
    assert unknown.state == "unknown" and unknown.unit is None


def test_service_registration_rejects_invalid_and_unavailable_targets(tmp_path):
    """
        确认非法命令样式和不可读目标无法创建登记记录。

        Args:
            tmp_path: 空的测试数据库目录。

        Returns:
            None: 未产生应用、服务或事件。
    """
    manager = Mock(spec=SystemdServiceManager)
    manager.inspect.return_value = sample("unknown", unit=None, detail="无法读取 systemd 服务状态")
    with TestClient(create_app(Config(data_dir=tmp_path), service_manager=manager)) as client:
        for unit in ("*.service", "--system.service", "/tmp/test.service", "detector.timer"):
            assert client.post("/services", json={"unit": unit}).status_code == 422
        manager.inspect.assert_not_called()
        assert client.post("/services", json={"unit": UNIT}).status_code == 409
        assert client.get("/services").json() == client.get("/incidents").json() == []
