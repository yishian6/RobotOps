"""
    全面复查发现的问题：每个用例对应实际状态/输入边界，不评测真实模型质量。
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from robotops_examples.pipeline.observer import PipelineObserver
from robotops_examples.pipeline.runtime import PipelineRuntime

from robotops.agent.context import ContextSnapshots
from robotops.agent.prompt_loader import load_investigation_prompt
from robotops.api import create_app
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import ConflictError, Store


def manual_report():
    """
        提供不依赖模型的固定升级报告。

        Args:
            无显式参数。

        Returns:
            dict: 最小 needs_human 报告，专用于存储终态和编排检查。
    """
    return {
        "status": "needs_human",
        "summary": "测试报告，证据不足。",
        "recommended_action": {"kind": "manual_review", "reason": "需要进一步观测"},
    }


def test_audit_artifacts_remain_readable_when_report_json_is_damaged(tmp_path):
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="camera", components=["camera"]))
    incident = store.create_incident(IncidentCreate(app_id="camera", symptom="no output"))
    run_id = store.start_run(incident["id"], "fixture")
    store.trace(run_id, {"type": "model"})
    sequence = ContextSnapshots(store, run_id).save([], [], {}, {})
    store.finish_run(run_id, report=manual_report())
    with store.connection() as db:
        db.execute("UPDATE runs SET report=? WHERE id=?", ("{", run_id))

    with pytest.raises(json.JSONDecodeError):
        store.run(run_id)
    assert store.trace(run_id)[0]["type"] == "model"
    assert ContextSnapshots(store, run_id).get(sequence)["messages"] == []


def test_core_accepts_real_component_names_without_example_defaults(tmp_path):
    """
        验证应用可声明真实业务组件且必须显式提供组件列表。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    with TestClient(create_app(Config(data_dir=tmp_path))) as client:
        body = {"app_id": "navigation", "components": ["planner", "controller"]}
        assert client.post("/applications", json=body).status_code == 201
        assert client.get("/applications/navigation").json()["components"] == body["components"]
        assert client.post("/applications", json={"app_id": "unspecified"}).status_code == 422


def test_prompt_input_stays_pinned_until_application_restart(tmp_path):
    """
        验证启动后修改磁盘模板不会改变已固定的模型输入及轨迹身份。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    snapshot = load_investigation_prompt()
    templates = {"system": snapshot.system, "task": snapshot.task}
    root = tmp_path / "prompts"
    folder = root / "investigation/v1"
    folder.mkdir(parents=True)

    def write_version(system):
        """
            改写磁盘候选文本以验证已启动服务不会重新读取它。

            Args:
                system: 本次写入的系统模板正文，任务模板取自外层固定快照。

            Returns:
                None: 覆盖测试目录中的模板和匹配的新清单。
        """
        current = {**templates, "system": system}
        for name, text in current.items():
            (folder / f"{name}.txt").write_text(text)
        manifest = {
            "name": "investigation",
            "version": "v1",
            "change_note": "回归测试模板。",
            "sha256": hashlib.sha256(
                json.dumps(current, ensure_ascii=False, sort_keys=True).encode()
            ).hexdigest(),
        }
        (folder / "manifest.json").write_text(json.dumps(manifest))

    write_version(templates["system"])

    def model(messages, tools):
        """
            检查输入仍来自启动快照，再返回协议测试报告。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                tools: 可调用工具的函数定义列表，包含名称、说明和参数 Schema。

            Returns:
                dict: 固定的合法升级报告消息。
        """
        assert "changed-after-startup" not in messages[0]["content"]
        return {"message": {"role": "assistant", "content": json.dumps(manual_report())}}

    app = create_app(
        Config(data_dir=tmp_path / "data", prompts_dir=root, prompt_version="v1"), complete=model
    )
    with TestClient(app) as client:
        pinned = client.get("/health").json()["prompt"]
        # 写入一份清单也合法的新模板，排除“因指纹不匹配读失败”造成未采用新内容的假通过。
        write_version(templates["system"] + "\nchanged-after-startup")
        assert (
            client.post(
                "/applications",
                json={"app_id": "pipeline", "components": ["producer", "processor"]},
            ).status_code
            == 201
        )
        incident = client.post("/incidents", json={"app_id": "pipeline", "symptom": "test"}).json()
        run = client.post(f"/incidents/{incident['id']}/investigate").json()
        assert run["state"] == "COMPLETED"
        snapshot = client.get(f"/runs/{run['id']}/trace").json()[0]
        assert snapshot["sha256"] == pinned["sha256"]


def test_failed_initialization_does_not_leave_runtime_marked_active(tmp_path, monkeypatch):
    """
        注入初始化写入失败，验证会话状态和已创建子进程都进入清理流程。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。
            monkeypatch: pytest 的可恢复替换工具，用于隔离环境变量、目录或依赖函数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    runtime = PipelineRuntime(tmp_path)
    from robotops_examples.pipeline import runtime as runtime_module

    def fail_write(*args, **kwargs):
        """
            替换写文件依赖，在创建子进程前主动制造确定性初始化故障。

            Args:
                *args: 原写函数的位置参数；本替身不使用其值。
                **kwargs: 原写函数的关键字参数；本替身不使用其值。

            Returns:
                None: 此替身不会成功返回。
        """
        raise OSError("injected configuration write failure")

    # 在第一次配置写入就失败，覆盖“尚未创建子进程但会话路径已经登记”的初始化边界。
    monkeypatch.setattr(runtime_module, "write_json", fail_write)
    with pytest.raises(OSError):
        runtime.start()
    assert runtime.runtime is None
    assert not runtime.processes


def test_terminal_run_cannot_be_overwritten_or_change_a_new_investigation(tmp_path):
    """
        验证旧运行的迟到完成请求不能覆盖既有结果或新调查的事件状态。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="test"))
    first = store.start_run(incident["id"], "test")
    store.finish_run(first, report=manual_report())
    second = store.start_run(incident["id"], "test")
    with pytest.raises(ConflictError):
        store.finish_run(first, error="late failure from the old run")
    assert store.run(first)["state"] == "COMPLETED"
    assert store.run(second)["state"] == "RUNNING"
    assert store.incident(incident["id"])["state"] == "INVESTIGATING"


def test_concurrent_finish_has_only_one_winner(tmp_path):
    """
        通过两个并发结束请求验证 RUNNING 到终态只有一次写入成功。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="pipeline", components=["producer", "processor"]))
    incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="test"))
    run = store.start_run(incident["id"], "test")

    def finish():
        """
            把每个结束竞争请求转换为可统计的成功标记。

            Args:
                无显式参数。

            Returns:
                bool: 成功更新终态为 True，预期状态冲突为 False。
        """
        try:
            store.finish_run(run, report=manual_report())
            return True
        except ConflictError:
            return False

    # 让两个请求真实竞争数据库状态条件，比串行重复调用更能覆盖检查后写入的并发竞态。
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _: finish(), range(2))) == 1


def test_bad_jsonl_does_not_hide_good_records_or_crash_queries(tmp_path):
    """
        混合有效、损坏和未完成日志，验证有效记录仍可检索且错误被明确计数。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    runtime = PipelineRuntime(tmp_path)
    runtime.runtime = tmp_path / "session"
    runtime.runtime.mkdir()
    row = {"at": "2026-11-17T00:00:00+00:00", "message": "useful failure"}
    (runtime.runtime / "producer.jsonl").write_bytes(
        json.dumps(row).encode() + b'\ninvalid-json\n{"at":"bad-time"}\n{"unfinished":'
    )
    result = PipelineObserver(runtime).search_logs("", 30, None)
    assert result["rows"] == [row]
    assert result["malformed_lines"] == 2
    assert result["incomplete_tail"] is True


@pytest.mark.parametrize("name", ["ROBOTOPS_DATA_DIR", "ROBOTOPS_PROMPTS_DIR"])
def test_empty_environment_paths_are_rejected(tmp_path, monkeypatch, name):
    """
        验证空白环境路径被拒绝，避免其默默解析为当前工作目录。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。
            monkeypatch: pytest 的可恢复替换工具，用于隔离环境变量、目录或依赖函数。
            name: 当前参数化的环境变量名，选择数据目录或 Prompt 目录。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ROBOTOPS_CONFIG", raising=False)
    monkeypatch.setenv(name, " ")
    with pytest.raises(ValueError):
        Config.load()


def test_invalid_endpoint_port_is_rejected_at_configuration_time():
    """
        验证非数字服务端口在配置构造时失败，避免调查中才发现地址不可用。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    with pytest.raises(ValueError):
        Config(model_base_url="http://localhost:invalid-port")
