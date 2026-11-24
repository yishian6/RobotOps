import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from pathlib import Path
from time import monotonic, sleep

import pytest

from robotops.agent.client import ModelError
from robotops.agent.context import ContextSnapshots
from robotops.config import Config
from robotops.models import ApplicationContract
from robotops.ros_facts import RosFact, investigate_fact, replay_facts, watch_facts
from robotops.storage import Store


def complete_with_ros_evidence(messages, _tools):
    fact = json.loads(messages[1]["content"])["incident"]["ros_fact"]
    assert fact["topic"] == "/smoke/output"
    return {"message": {"role": "assistant", "content": json.dumps({
        "status": "needs_human", "summary": "Topic stopped delivering messages",
        "hypotheses": [{
            "cause": "Exact cause unknown", "assessment": "unverified",
            "evidence_ids": [fact["evidence_id"]],
        }],
        "recommended_action": {"kind": "manual_review", "reason": "Check the publisher"},
    })}}


class ScriptedClient:
    def __init__(self, _config):
        pass

    complete = staticmethod(complete_with_ros_evidence)


@pytest.fixture
def topic_fact():
    return RosFact.model_validate({
        "source_id": "a" * 32, "app_id": "camera", "topic": "/smoke/output",
        "sequence": 1, "kind": "topic_silent", "observed_at_ms": 1700000003000,
        "last_message_at_ms": 1700000000000, "silent_ms": 3000, "threshold_ms": 2000,
    }).model_dump()


def test_ros_fact_is_dispatched_once_with_original_evidence(tmp_path, topic_fact):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: store.ingest_ros_fact("camera", topic_fact), range(2)))
    incident_id, = [result for result in results if result]
    assert store.ingest_ros_fact("camera", topic_fact) is None

    run = investigate_fact(
        store, Config(data_dir=tmp_path), incident_id, complete_with_ros_evidence, 6,
    )
    assert run["state"] == "COMPLETED"
    assert store.incident(incident_id)["automatic_run_id"] == run["id"]
    evidence_id, = run["report"]["hypotheses"][0]["evidence_ids"]
    assert store.read_evidence(run["id"], evidence_id)["raw"]["data"] == topic_fact
    assert not list((tmp_path / "evidence").iterdir())

    with store.connection() as db:
        db.execute(
            "UPDATE ros_fact_tasks SET fact=json_set(fact, '$.silent_ms', ?)", (4000,),
        )
    with pytest.raises(ValueError, match="Evidence integrity check failed"):
        store.read_evidence(run["id"], evidence_id)


def write_fact_database(path, topic_fact, sequences):
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA application_id=1380930374;
            CREATE TABLE source(slot INTEGER, id TEXT, app TEXT, topic TEXT);
            CREATE TABLE topic_silences(
                sequence INTEGER PRIMARY KEY, observed_at_ms INTEGER,
                last_message_at_ms INTEGER, silent_ms INTEGER, threshold_ms INTEGER
            );
        """)
        db.execute("INSERT INTO source VALUES(1, ?, ?, ?)", (
            topic_fact["source_id"], topic_fact["app_id"], topic_fact["topic"],
        ))
        for sequence in sequences:
            db.execute("INSERT INTO topic_silences VALUES(?, ?, ?, ?, ?)", (
                sequence, topic_fact["observed_at_ms"], topic_fact["last_message_at_ms"],
                topic_fact["silent_ms"], topic_fact["threshold_ms"],
            ))


def test_worker_replays_missing_facts_and_pending_tasks_after_restart(tmp_path, topic_fact, monkeypatch):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    queued = store.ingest_ros_fact("camera", {**topic_fact, "sequence": 2})
    fact_db = tmp_path / "ros-facts.db"
    write_fact_database(fact_db, topic_fact, (1, 2, 3))
    assert replay_facts(store, "camera", fact_db, limit=1) == 1
    assert len(store.incidents()) == 2

    path = tmp_path / "facts.sock"
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as abandoned:
        abandoned.bind(str(path))
    assert path.is_socket()
    monkeypatch.setattr("robotops.ros_facts.DeepSeekClient", ScriptedClient)
    monkeypatch.setattr("robotops.ros_facts.REPLAY_INTERVAL_SECONDS", 0.05)
    config = Config(data_dir=tmp_path)
    assert watch_facts(config, "camera", path, once=True, max_model_calls=6) == 0
    assert store.incident(queued)["automatic_task_state"] == "COMPLETED"
    pending, last = store.queued_ros_fact_incidents("camera")
    assert store.incident(pending)["ros_fact"] == topic_fact
    assert watch_facts(config, "camera", path, once=True, max_model_calls=6) == 0
    assert store.incident(pending)["automatic_task_state"] == "COMPLETED"
    assert store.incident(last)["automatic_task_state"] == "QUEUED"
    assert watch_facts(config, "camera", path, once=True, max_model_calls=6) == 0
    assert store.incident(last)["automatic_task_state"] == "COMPLETED"
    assert replay_facts(store, "camera", fact_db) == 0
    assert len(store.incidents()) == 3


def test_live_worker_picks_up_a_fact_without_a_datagram(tmp_path, topic_fact, monkeypatch):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    path = tmp_path / "facts.sock"
    fact_db = tmp_path / "ros-facts.db"
    monkeypatch.setattr("robotops.ros_facts.DeepSeekClient", ScriptedClient)
    monkeypatch.setattr("robotops.ros_facts.REPLAY_INTERVAL_SECONDS", 0.05)
    with ThreadPoolExecutor(max_workers=1) as receiver:
        task = receiver.submit(
            watch_facts, Config(data_dir=tmp_path), "camera", path, once=True, max_model_calls=6,
        )
        try:
            wait_until(path.is_socket)
            write_fact_database(fact_db, topic_fact, (1,))
            assert task.result(timeout=4) == 0
        finally:
            if not task.done() and path.is_socket():
                with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sender:
                    sender.sendto(json.dumps(topic_fact).encode(), str(path))
    incident, = store.incidents()
    assert store.incident(incident["id"])["automatic_task_state"] == "COMPLETED"


@pytest.mark.parametrize("limit, expected_calls, reason", [
    (2, 2, "model_call_budget"), (6, 3, "update_working_memory"),
])
def test_fallback_report_cites_trigger_observation(tmp_path, topic_fact, limit, expected_calls, reason):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    call_count = 0

    def complete(_messages, tools):
        nonlocal call_count
        call_count += 1
        if [tool["function"]["name"] for tool in tools] == ["submit_report"]:
            raise ModelError("Model could not finish the report")
        return {"message": {"role": "assistant", "tool_calls": [{
            "id": f"memory-{call_count}", "type": "function",
            "function": {"name": "update_working_memory", "arguments": "{}"},
        }]}}

    incident_id = store.ingest_ros_fact("camera", topic_fact)
    run = investigate_fact(store, Config(data_dir=tmp_path), incident_id, complete, limit)
    assert run["state"] == "COMPLETED"
    assert run["report"]["status"] == "needs_human"
    assert call_count == expected_calls
    snapshots = ContextSnapshots(store, run["id"])
    assert len(snapshots.list()) == expected_calls
    assert snapshots.get(expected_calls)["manifest"]["report_only"] == reason
    assert snapshots.get(expected_calls)["memory"]["version"] == 0
    evidence_id, = run["report"]["recommended_action"]["evidence_ids"]
    assert store.read_evidence(run["id"], evidence_id)["tool"] == "ros1_topic_observation"


def local_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_until(predicate, timeout=10):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        result = predicate()
        if result:
            return result
        sleep(0.05)
    pytest.fail("Timed out waiting for the ROS 1 fixture")


def stop_process(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)


@pytest.mark.skipif(
    not os.environ.get("ROBOTOPS_ROS1_FACTS_BIN"),
    reason="Requires a built collector and a sourced ROS 1 Noetic environment",
)
def test_live_ros1_dual_database_chain(tmp_path, monkeypatch, capsys):
    binary = Path(os.environ["ROBOTOPS_ROS1_FACTS_BIN"]).resolve(strict=True)
    fact_db = tmp_path / "ros-facts.db"
    config = Config(data_dir=tmp_path / "platform")
    store = Store(config.data_dir)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    master_port = local_port()
    socket_path = tmp_path / "facts.sock"
    env = {
        name: os.environ[name] for name in (
            "PATH", "LANG", "LC_ALL", "PYTHONPATH", "LD_LIBRARY_PATH", "CMAKE_PREFIX_PATH",
            "ROS_DISTRO", "ROS_VERSION", "ROS_PYTHON_VERSION", "ROS_ETC_DIR", "ROS_ROOT",
            "ROS_PACKAGE_PATH",
        ) if name in os.environ
    }
    env.update(
        ROS_MASTER_URI=f"http://127.0.0.1:{master_port}", ROS_IP="127.0.0.1",
        ROS_HOME=str(tmp_path / "ros-home"), ROS_LOG_DIR=str(tmp_path / "ros-logs"),
    )

    with ExitStack() as cleanup:
        def launch(command, name):
            output = cleanup.enter_context((tmp_path / (name + ".log")).open("w"))
            process = subprocess.Popen(
                command, env=env, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            cleanup.callback(stop_process, process)
            return process

        master = launch(["roscore", "-p", str(master_port)], "roscore")
        monkeypatch.setattr("robotops.ros_facts.DeepSeekClient", ScriptedClient)
        receiver = cleanup.enter_context(ThreadPoolExecutor(max_workers=1))
        task = receiver.submit(watch_facts, config, "camera", socket_path, once=True, max_model_calls=6)
        def release_receiver():
            if task.done() or not socket_path.is_socket():
                return
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sender:
                sender.sendto(json.dumps({
                    "source_id": "b" * 32, "app_id": "camera", "topic": "/smoke/output",
                    "sequence": 1, "kind": "topic_silent", "observed_at_ms": 1700000003000,
                    "last_message_at_ms": 1700000000000, "silent_ms": 3000, "threshold_ms": 2000,
                }).encode(), str(socket_path))

        cleanup.callback(release_receiver)
        wait_until(socket_path.is_socket)
        collector = launch(
            [str(binary), "camera", "/smoke/output", "500", str(socket_path), str(fact_db)],
            "collector",
        )

        code = """
import pathlib
import sys
import time

import rospy
from std_msgs.msg import String

root = pathlib.Path(sys.argv[1])
rospy.init_node("robotops_fact_fixture")
pub = rospy.Publisher("/smoke/output", String, queue_size=1, latch=True)
while not rospy.is_shutdown():
    if not (root / "pause").exists():
        pub.publish("fixture output")
        if pub.get_num_connections():
            (root / "ready").touch()
    time.sleep(0.025)
"""
        publisher = launch(["/usr/bin/python3", "-c", code, str(tmp_path)], "publisher")
        wait_until((tmp_path / "ready").exists)
        sleep(0.6)
        assert not store.incidents()

        (tmp_path / "pause").touch()
        assert task.result(timeout=10) == 0
        assert publisher.poll() is None and collector.poll() is None and master.poll() is None
        run = json.loads(capsys.readouterr().out)
        assert run["state"] == "COMPLETED"
        evidence_id, = run["report"]["hypotheses"][0]["evidence_ids"]
        observation = store.read_evidence(run["id"], evidence_id)["raw"]["data"]
        assert not (store.root / "evidence" / f"{evidence_id}.json").exists()
        assert not socket_path.exists()
        sleep(0.6)
        assert len(store.incidents()) == 1

        with closing(sqlite3.connect(f"{fact_db.as_uri()}?mode=ro", uri=True)) as database:
            database.row_factory = sqlite3.Row
            source = database.execute("SELECT id, app, topic FROM source").fetchone()
            facts = database.execute("SELECT * FROM topic_silences").fetchall()
            fact, = facts
            assert observation == {
                **dict(fact), "source_id": source["id"], "app_id": source["app"],
                "topic": source["topic"], "kind": "topic_silent",
            }
            assert database.execute(
                "SELECT name FROM sqlite_master WHERE name='incidents'",
            ).fetchone() is None
        with store.connection() as database:
            assert database.execute(
                "SELECT name FROM sqlite_master WHERE name='topic_silences'",
            ).fetchone() is None


@pytest.mark.skipif(
    not os.environ.get("ROBOTOPS_ROS1_FACTS_BIN"),
    reason="Requires a sourced ROS 1 Noetic environment",
)
@pytest.mark.parametrize("always_fail", [False, True], ids=["recover", "retry-fails"])
def test_live_ros1_required_node_exit_restarts_the_same_launch_once(tmp_path, always_fail):
    package = tmp_path / "robotops_restart_fixture"
    package.mkdir()
    (package / "package.xml").write_text(
        '<package format="2"><name>robotops_restart_fixture</name><version>0.0.0</version>'
        '<description>RobotOps fixture</description><maintainer email="test@example.com">Test</maintainer>'
        '<license>MIT</license></package>'
    )
    node = package / "fixture.py"
    node.write_text(
        "#!/usr/bin/python3\n"
        "from pathlib import Path\n"
        "import sys\n"
        "from time import sleep\n"
        "marker = Path(sys.argv[1])\n"
        f"if {always_fail} or not marker.exists():\n"
        "    marker.touch()\n"
        "    print('first launch fails', flush=True)\n"
        "    sys.exit(17)\n"
        "sleep(60)\n"
    )
    node.chmod(0o700)
    launch = package / "fixture.launch"
    launch.write_text(
        '<launch><node pkg="robotops_restart_fixture" type="fixture.py" name="fixture" '
        f'required="true" args="{tmp_path / "ran-once"}" /></launch>'
    )
    port = local_port()
    store = Store(tmp_path / "data")
    env = {
        **os.environ, "ROBOTOPS_DATA_DIR": str(store.root), "ROS_IP": "127.0.0.1",
        "ROS_MASTER_URI": f"http://127.0.0.1:{port}", "ROS_HOME": str(tmp_path / "ros-home"),
        "ROS_LOG_DIR": str(tmp_path / "ros-logs"),
        "ROS_PACKAGE_PATH": os.pathsep.join((str(tmp_path), os.environ.get("ROS_PACKAGE_PATH", ""))),
    }
    env.pop("DEEPSEEK_API_KEY", None)
    env.pop("ROS_HOSTNAME", None)
    with ExitStack() as cleanup:
        for name, command in (
            ("roscore", ["roscore", "-p", str(port)]),
            ("runner", [
                sys.executable, "-m", "robotops.cli", "run", "--restart-on-exit", "fixture",
                "--", "roslaunch", "--wait", "--screen", str(launch),
            ]),
        ):
            log = cleanup.enter_context((tmp_path / f"{name}.log").open("w"))
            process = subprocess.Popen(
                command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            cleanup.callback(stop_process, process)

        def stable():
            assert process.poll() is None, (tmp_path / "runner.log").read_text()
            for row in store.incidents():
                event = store.incident(row["id"])
                if event.get("launch_restart", {}).get("state") == "STABLE":
                    return event
            return None

        if always_fail:
            assert process.wait(timeout=25) == 17, (tmp_path / "runner.log").read_text()
            events = [store.incident(row["id"]) for row in store.incidents()]
            original, = [event for event in events if "launch_restart" in event]
            assert original["launch_restart"]["state"] == "FAILED"
            assert len(events) == 2
        else:
            event = wait_until(stable, timeout=25)
            assert event["state"] == "OPEN"
            assert event["execution_exit"]["exit_code"] == 17
            assert event["launch_restart"]["execution_id"] != event["execution_exit"]["execution_id"]
            assert any("first launch fails" in line for line in store.execution_logs(event["id"])["lines"])
            stop_process(process)
            assert len(store.incidents()) == 1
        with store.connection() as db:
            assert db.execute("SELECT count(*) FROM executions").fetchone()[0] == 2
