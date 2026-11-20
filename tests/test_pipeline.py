"""
    独立 pipeline 示例的真实进程、观测与生命周期集成检查。
"""

import json
import subprocess
import sys

from fastapi.testclient import TestClient
from robotops_examples.pipeline.app import create_app
from robotops_examples.pipeline.runtime import process_info

from robotops.config import Config


def test_real_demo_faults_and_tools():
    # 从示例自己的入口运行，验证命令分派及实际故障；不借助核心 CLI。
    """
        从示例自己的 CLI 运行真实故障烟测，验证全部场景且没有调用模型。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    completed = subprocess.run(
        [sys.executable, "-m", "robotops_examples.pipeline", "smoke"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    result = json.loads(completed.stdout)
    assert result["status"] == "passed"
    assert result["model_called"] is False
    assert set(result["checks"]) == {
        "normal",
        "input_stopped",
        "processing_stalled",
        "config_mismatch",
        "processor_exit",
    }


def test_demo_api_shuts_down_owned_children(tmp_path):
    """
        验证重复启动、非法故障参数和组件退出，并确认服务关闭后回收其子进程。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    app = create_app(Config(data_dir=tmp_path))
    with TestClient(app) as client:
        started = client.post("/demo/start")
        assert started.status_code == 201
        pids = [item["process"]["pid"] for item in started.json()["components"].values()]
        assert client.post("/demo/start").status_code == 409
        assert client.post("/demo/fault", json={"mode": "unsupported"}).status_code == 422
        assert client.post("/demo/fault", json={"mode": "processor_exit"}).status_code == 200
        assert not client.get("/demo").json()["components"]["processor"]["process"]["alive"]
    assert not any(process_info(pid)["alive"] for pid in pids)


def test_demo_registers_configured_contract(tmp_path):
    """
        验证示例使用显式契约文件注册应用，配置描述确实进入数据库。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    path = tmp_path / "pipeline.json"
    path.write_text(
        json.dumps(
            {
                "app_id": "pipeline",
                "components": ["producer", "processor"],
                "description": "配置契约实际生效",
            }
        )
    )
    app = create_app(Config(data_dir=tmp_path / "data"), contract_file=path)
    with TestClient(app) as client:
        assert client.post("/demo/start").status_code == 201
        assert client.get("/applications/pipeline").json()["description"] == "配置契约实际生效"
