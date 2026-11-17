"""职责边界回归检查：核心可独立启动，示例控制和模板资源分别管理。"""

import subprocess
import sys

import pytest
from fastapi.testclient import TestClient

from robotops.agent.prompt_loader import load_investigation_prompt
from robotops.api import create_app
from robotops.config import Config


def test_core_has_no_demo_control_routes(tmp_path):
    with TestClient(create_app(Config(data_dir=tmp_path))) as client:
        assert client.get("/health").status_code == 200
        assert client.post("/demo/start").status_code == 404
        assert client.post("/demo/fault", json={"mode": "processor_exit"}).status_code == 404


def test_core_imports_without_example_package(tmp_path):
    # 阻止示例包导入，比只搜索 import 字符串更能发现间接/动态反向依赖。
    code = """import importlib.abc
import sys
from pathlib import Path
class BlockExamples(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if any(fullname == name or fullname.startswith(name + ".")
               for name in ("robotops_examples", "robotops_benchmarks")):
            raise AssertionError("Core imported an example or benchmark package")
sys.meta_path.insert(0, BlockExamples())
from robotops.api import create_app
from robotops.cli import main
from robotops.config import Config
app = create_app(Config(data_dir=Path(sys.argv[1])))
assert not any(route.path.startswith("/demo") for route in app.routes)
# 不只检查 import：执行 CLI 的全部有效分支，防止局部导入绕过依赖边界检查。
import uvicorn
calls = []
uvicorn.run = lambda *args, **kwargs: calls.append((args, kwargs))
sys.argv = ["robotops", "serve", "--port", "8123"]
main()
assert calls == [(("robotops.api:create_app",), {"factory": True, "host": "127.0.0.1", "port": 8123})]
sys.argv = ["robotops", "check-prompts"]
main()
for removed in (["smoke"], ["serve", "--example", "pipeline"]):
    sys.argv = ["robotops", *removed]
    try:
        main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Core CLI still accepts example commands")
"""
    subprocess.run([sys.executable, "-c", code, str(tmp_path / "data")], cwd=tmp_path, check=True)


def test_explicit_prompt_directory_does_not_fall_back(tmp_path):
    with pytest.raises(ValueError, match="unknown investigation prompt"):
        load_investigation_prompt("v1", tmp_path / "missing-prompts")
