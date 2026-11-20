"""
    Prompt 发布的确定性检查；不将模板/协议测试当成模型质量评测。
"""

import hashlib
import json
from importlib.resources import files

import pytest
from fastapi.testclient import TestClient

from robotops.agent import prompt_loader as prompts
from robotops.api import create_app
from robotops.config import Config


@pytest.fixture
def candidate_prompt(tmp_path, monkeypatch):
    """
        创建与内置 v1 不同的候选版本并隔离资源定位，验证显式选版。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。
            monkeypatch: pytest 的可恢复替换工具，用于隔离环境变量、目录或依赖函数。

        Returns:
            Path: 含 v2 模板与匹配清单的候选目录；资源定位替换由 monkeypatch 自动恢复。
    """
    snapshot = prompts.load_investigation_prompt("v1")
    templates = {"system": snapshot.system, "task": snapshot.task}
    templates["system"] += "\n候选版本的测试标记。\n"
    folder = tmp_path / "prompts" / "investigation" / "v2"
    folder.mkdir(parents=True)
    for name, content in templates.items():
        (folder / f"{name}.txt").write_text(content, encoding="utf-8")
    manifest = {
        "name": "investigation",
        "version": "v2",
        "sha256": hashlib.sha256(
            json.dumps(templates, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest(),
        "change_note": "测试候选版本，不是真实模型效果结果。",
    }
    (folder / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(prompts, "files", lambda _: tmp_path)
    return folder


def test_selected_version_reaches_api_model_and_trace(candidate_prompt, tmp_path, monkeypatch):
    """
        验证环境选定的候选 Prompt 版本贯穿配置、健康接口、模型输入和运行轨迹。

        Args:
            candidate_prompt: 候选 Prompt 目录夹具，包含测试清单、模板和替换后的资源定位器。
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。
            monkeypatch: pytest 的可恢复替换工具，用于隔离环境变量、目录或依赖函数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    config = tmp_path / "robotops.toml"
    config.write_text('prompt_version = "v1"\n')
    monkeypatch.setenv("ROBOTOPS_CONFIG", str(config))
    # 文件选 v1、环境选 v2，随后检查健康接口、真实调用输入与轨迹身份是否一致采用覆盖值。
    monkeypatch.setenv("ROBOTOPS_PROMPT_VERSION", "v2")
    monkeypatch.setenv("ROBOTOPS_DATA_DIR", str(tmp_path / "data"))
    settings = Config.load()
    assert settings.prompt_version == "v2"

    def complete(messages, tools):
        """
            检查候选模板确实进入实际调用，再返回协议用报告。

            Args:
                messages: 按协议排列的消息列表，包含系统约束、事件以及助手与工具的历史交互。
                tools: 可调用工具的函数定义列表，包含名称、说明和参数 Schema。

            Returns:
                dict: 合法的固定升级报告助手消息。
        """
        assert "候选版本的测试标记" in messages[0]["content"]
        return {
            "message": {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "status": "needs_human",
                        "summary": "测试编排，不评测模型效果。",
                        "hypotheses": [],
                        "recommended_action": {"kind": "manual_review", "reason": "需要更多证据"},
                        "unresolved_questions": [],
                    }
                ),
            }
        }

    with TestClient(create_app(settings, complete=complete)) as client:
        assert client.get("/health").json()["prompt"]["version"] == "investigation/v2"
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
        assert snapshot["version"] == "investigation/v2"
        assert "候选版本的测试标记" in snapshot["messages"][0]["content"]


def test_changed_content_requires_manifest_update(candidate_prompt):
    """
        验证模板正文被修改而清单未更新时指纹校验失败。

        Args:
            candidate_prompt: 候选 Prompt 目录夹具，包含测试清单、模板和替换后的资源定位器。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    path = candidate_prompt / "system.txt"
    path.write_text(path.read_text() + "unexpected change")
    with pytest.raises(ValueError, match="manifest/content mismatch"):
        prompts.load_investigation_prompt("v2")


def test_wrong_manifest_version_is_rejected(candidate_prompt):
    """
        验证目录版本与清单版本不一致时拒绝加载。

        Args:
            candidate_prompt: 候选 Prompt 目录夹具，包含测试清单、模板和替换后的资源定位器。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    path = candidate_prompt / "manifest.json"
    manifest = json.loads(path.read_text())
    path.write_text(json.dumps({**manifest, "version": "v3"}))
    with pytest.raises(ValueError, match="manifest/content mismatch"):
        prompts.load_investigation_prompt("v2")


@pytest.mark.parametrize("version", ["../v1", "latest", "production", "v999999"])
def test_unknown_or_unpinned_versions_do_not_fall_back(version):
    """
        验证路径穿越、别名及不存在的版本不会回退为其他模板。

        Args:
            version: 当前参数化的路径、非固定别名或不存在版本，不应被隐式回退。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    with pytest.raises(ValueError):
        prompts.load_investigation_prompt(version)


def test_invalid_prompt_fails_before_database_creation(tmp_path):
    """
        验证未知 Prompt 在初始化业务存储前失败，不遗留半初始化数据库目录。

        Args:
            tmp_path: pytest 提供的独立临时目录，隔离本用例的数据库、证据和文件资源。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    root = tmp_path / "data"
    with pytest.raises(ValueError, match="unknown investigation prompt"):
        create_app(Config(data_dir=root, prompt_version="v999999"))
    assert not root.exists()


def test_all_packaged_prompt_versions_match_their_manifests():
    """
        遍历内置模板版本，验证其清单指纹和显式版本身份一致。

        Args:
            无显式参数。

        Returns:
            None: 全部断言与预期异常检查通过时正常结束。
    """
    root = files("robotops.resources").joinpath("prompts", "investigation")
    for directory in root.iterdir():
        if directory.is_dir():
            metadata = prompts.load_investigation_prompt(directory.name).metadata
            assert metadata["version"] == f"investigation/{directory.name}"
