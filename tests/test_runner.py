"""
    覆盖一次命令启动到 Agent 证据的正常入口，全程用本地模型替身。
"""

import json
import os
import subprocess
import sys

from fastapi.testclient import TestClient

from robotops.api import create_app
from robotops.config import Config
from robotops.storage import Store


def test_topic_acceptance_closes_only_the_original_incident(tmp_path):
    store = Store(tmp_path)
    store.start_execution("camera", "old-execution")
    original = store.record_execution_exit("old-execution", "roslaunch", 17, 0)
    other = store.record_execution_exit("old-execution", "another-node", 19, 0)
    store.start_execution(
        "camera", "new-execution", restart_of=original,
        topic_acceptance={"state": "PENDING", "topic": "/camera/output"},
    )
    run = store.start_run(original, "fixture")
    passed = {"state": "PASSED", "topic": "/camera/output"}
    store.finish_topic_acceptance("new-execution", passed)
    assert store.incident(original)["launch_restart"]["topic_acceptance"]["state"] == "PENDING"

    store.finish_launch_restart("new-execution", "STABLE")
    assert store.incident(original)["state"] == "INVESTIGATING"
    store.finish_topic_acceptance("new-execution", passed)
    store.finish_topic_acceptance("new-execution", {"state": "FAILED"})
    event = store.incident(original)
    assert event["state"] == "RESOLVED"
    assert event["launch_restart"]["topic_acceptance"]["state"] == "PASSED"
    assert store.incident(other)["state"] == "OPEN"

    store.finish_run(run, error="A late investigation failed")
    follow_up = store.start_run(original, "fixture")
    assert store.incident(original)["state"] == "RESOLVED"
    store.finish_run(follow_up, report={"status": "proposed"})
    assert store.run(follow_up)["state"] == "COMPLETED"
    assert store.incident(original)["state"] == "RESOLVED"


def test_console_exit_is_available_to_investigation(tmp_path):
    """
        验证终端输出、退出事件和调查报告引用同一次运行，正常退出不新建事件。

        Args:
            tmp_path: 隔离本次运行的 SQLite 和日志目录。

        Returns:
            None: CLI、查询 API 和调查工具共用同一份故障日志。
    """
    env = {**os.environ, "ROBOTOPS_DATA_DIR": str(tmp_path)}
    env.pop("DEEPSEEK_API_KEY", None)
    command = [sys.executable, "-m", "robotops.cli", "run", "my-worker", "--", sys.executable]
    failure = subprocess.run(
        [*command, "-c", 'import sys; print("sensor absent", file=sys.stderr); sys.exit(17)'],
        env=env, capture_output=True, text=True, timeout=10,
    )
    success = subprocess.run(
        [*command, "-c", 'print("next run")'],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert failure.returncode == 17 and "sensor absent" in failure.stdout
    assert success.returncode == 0 and "next run" in success.stdout
    store = Store(tmp_path)
    event, = store.incidents()
    incident_id = event["id"]
    assert incident_id in failure.stderr

    def complete(messages, _tools):
        """
            先找错误日志，再用这份证据提交结论。

            Args:
                messages: 本轮装配后的真实调查消息。
                _tools: 当前工具定义，此测试只验证输出的完整链路。

            Returns:
                dict: 模型工具调用或结构化报告的替身响应。
        """
        if messages[-1]["role"] != "tool":
            return {"message": {"role": "assistant", "tool_calls": [{
                "id": "log-1", "type": "function",
                "function": {"name": "search_logs", "arguments": '{"keyword":"SENSOR"}'},
            }]}}
        evidence = json.loads(messages[-1]["content"])
        assert evidence["data"]["lines"] == ["sensor absent"]
        return {"message": {"role": "assistant", "content": json.dumps({
            "status": "proposed", "summary": "程序报告传感器不可用",
            "hypotheses": [{
                "cause": "退出前报告 sensor absent", "assessment": "supported",
                "evidence_ids": [evidence["evidence_id"]],
            }],
            "recommended_action": {"kind": "manual_review", "reason": "核对传感器"},
        })}}

    with TestClient(create_app(Config(data_dir=tmp_path), complete=complete)) as client:
        assert client.get(f"/incidents/{incident_id}").json()["execution_exit"]["exit_code"] == 17
        log = client.get(f"/incidents/{incident_id}/logs").json()
        assert log["lines"] == ["sensor absent"]
        run = client.post(f"/incidents/{incident_id}/investigate").json()
        assert run["state"] == "COMPLETED", run
        evidence_id = run["report"]["hypotheses"][0]["evidence_ids"][0]
        original = client.get(f"/runs/{run['id']}/evidence/{evidence_id}").json()
        assert original["raw"]["data"] == log
