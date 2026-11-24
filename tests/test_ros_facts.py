import json
import os
import signal
import socket
import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from http.client import HTTPConnection
from pathlib import Path
from time import monotonic, sleep

import pytest

from robotops.agent.client import ModelError
from robotops.config import Config
from robotops.models import ApplicationContract
from robotops.ros_facts import FactBatch, dispatch_facts, fetch_facts, watch_facts
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
def topic_batch():
    return FactBatch.model_validate({
        "source_id": "a" * 32, "app_id": "camera", "topic": "/smoke/output",
        "events": [{
            "sequence": 1, "kind": "topic_silent", "observed_at_ms": 1700000003000,
            "last_message_at_ms": 1700000000000, "silent_ms": 3000, "threshold_ms": 2000,
        }],
    })


def test_ros_fact_is_dispatched_once_with_original_evidence(tmp_path, topic_batch):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    batch = topic_batch

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda _: dispatch_facts(
                store, Config(data_dir=tmp_path), batch, complete_with_ros_evidence,
            ),
            range(2),
        ))

    run, = [run for result in results for run in result]
    incident, = store.incidents()
    assert run["state"] == "COMPLETED"
    assert store.incident(incident["id"])["automatic_run_id"] == run["id"]
    evidence_id, = run["report"]["hypotheses"][0]["evidence_ids"]
    raw = store.read_evidence(run["id"], evidence_id)["raw"]["data"]
    assert raw == {**batch.events[0].model_dump(), "source_id": batch.source_id,
                   "app_id": batch.app_id, "topic": batch.topic}
    assert dispatch_facts(store, Config(data_dir=tmp_path), batch, complete_with_ros_evidence) == []


def test_idle_fact_poll_needs_neither_a_write_lock_nor_prompt(tmp_path, topic_batch, monkeypatch):
    store = Store(tmp_path)
    original_connection = store.connection

    @contextmanager
    def connection_without_waiting():
        with original_connection() as db:
            db.execute("PRAGMA busy_timeout=0")
            yield db

    def unexpected_call(*_):
        pytest.fail("An idle worker should neither read the prompt nor call the model")

    monkeypatch.setattr(store, "connection", connection_without_waiting)
    monkeypatch.setattr("robotops.ros_facts.load_investigation_prompt", unexpected_call)
    idle = topic_batch.model_copy(update={"events": []})
    with original_connection() as writer:
        writer.execute("BEGIN IMMEDIATE")
        assert dispatch_facts(store, Config(data_dir=tmp_path), idle, unexpected_call) == []


def test_worker_runs_persisted_task_while_collector_is_unavailable(
    tmp_path, topic_batch, monkeypatch, capsys,
):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    scope = topic_batch.model_dump(exclude={"events"})
    store.ingest_ros_facts("camera", [{**scope, **topic_batch.events[0].model_dump()}])
    incident, = store.incidents()

    def offline_collector(*_):
        raise ConnectionRefusedError("collector offline")

    monkeypatch.setattr("robotops.ros_facts.fetch_facts", offline_collector)
    monkeypatch.setattr("robotops.ros_facts.DeepSeekClient", ScriptedClient)
    config = Config(data_dir=tmp_path)

    assert watch_facts(config, "camera", 8081, once=True, max_model_calls=6) == 1
    output = capsys.readouterr()
    assert "collector offline" in output.err
    run = json.loads(output.out)
    assert run["state"] == "COMPLETED"
    assert store.incident(incident["id"])["automatic_run_id"] == run["id"]
    evidence_id, = run["report"]["hypotheses"][0]["evidence_ids"]
    assert store.read_evidence(run["id"], evidence_id)["raw"]["data"]["source_id"] == scope["source_id"]
    assert store.pending_ros_facts("camera") == []
    assert len(store.incidents()) == 1


def test_fallback_report_cites_trigger_observation(tmp_path, topic_batch):
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
            "function": {"name": "update_working_memory", "arguments": '{"phase":"invalid"}'},
        }]}}

    run, = dispatch_facts(store, Config(data_dir=tmp_path), topic_batch, complete)
    assert run["state"] == "COMPLETED"
    assert run["report"]["status"] == "needs_human"
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
    master_port, port = local_port(), local_port()
    while port == master_port:
        port = local_port()
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

    def read(path):
        connection = HTTPConnection("127.0.0.1", port, timeout=1)
        try:
            connection.request("GET", path)
            with connection.getresponse() as response:
                assert response.status == 200
                return json.load(response)
        finally:
            connection.close()

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
        collector = launch(
            [str(binary), "camera", "/smoke/output", "500", str(port), str(fact_db)], "collector",
        )

        def source_ready():
            assert master.poll() is None and collector.poll() is None
            try:
                return read("/v1/source")
            except OSError:
                return None

        source = wait_until(source_ready)
        assert source["topic"] == "/smoke/output"
        assert not fetch_facts(store, "camera", port).events
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
        assert not fetch_facts(store, "camera", port).events
        assert not store.incidents()

        (tmp_path / "pause").touch()

        def outage():
            batch = fetch_facts(store, "camera", port)
            return batch if batch.events else None

        batch = wait_until(outage)
        assert publisher.poll() is None
        assert len(batch.events) == 1
        monkeypatch.setattr("robotops.ros_facts.DeepSeekClient", ScriptedClient)
        assert watch_facts(config, "camera", port, once=True, max_model_calls=6) == 0
        run = json.loads(capsys.readouterr().out)
        assert run["state"] == "COMPLETED"
        evidence_id, = run["report"]["hypotheses"][0]["evidence_ids"]
        observation = store.read_evidence(run["id"], evidence_id)["raw"]["data"]
        assert read(f"/v1/event/{observation['sequence']}") == observation
        sleep(0.6)
        assert not fetch_facts(store, "camera", port).events
        assert len(store.incidents()) == 1

        with closing(sqlite3.connect(f"{fact_db.as_uri()}?mode=ro", uri=True)) as database:
            assert database.execute("SELECT count(*) FROM topic_silences").fetchone()[0] == 1
            assert database.execute(
                "SELECT name FROM sqlite_master WHERE name='incidents'",
            ).fetchone() is None
        with store.connection() as database:
            assert database.execute(
                "SELECT name FROM sqlite_master WHERE name='topic_silences'",
            ).fetchone() is None
