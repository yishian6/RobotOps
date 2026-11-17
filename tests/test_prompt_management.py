"""Prompt 发布的确定性检查；不将模板/协议测试当成模型质量评测。"""

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
    config = tmp_path / "robotops.toml"
    config.write_text('prompt_version = "v1"\n')
    monkeypatch.setenv("ROBOTOPS_CONFIG", str(config))
    monkeypatch.setenv("ROBOTOPS_PROMPT_VERSION", "v2")
    monkeypatch.setenv("ROBOTOPS_DATA_DIR", str(tmp_path / "data"))
    settings = Config.load()
    assert settings.prompt_version == "v2"

    def complete(messages, tools):
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
    path = candidate_prompt / "system.txt"
    path.write_text(path.read_text() + "unexpected change")
    with pytest.raises(ValueError, match="manifest/content mismatch"):
        prompts.load_investigation_prompt("v2")


def test_wrong_manifest_version_is_rejected(candidate_prompt):
    path = candidate_prompt / "manifest.json"
    manifest = json.loads(path.read_text())
    path.write_text(json.dumps({**manifest, "version": "v3"}))
    with pytest.raises(ValueError, match="manifest/content mismatch"):
        prompts.load_investigation_prompt("v2")


@pytest.mark.parametrize("version", ["../v1", "latest", "production", "v999999"])
def test_unknown_or_unpinned_versions_do_not_fall_back(version):
    with pytest.raises(ValueError):
        prompts.load_investigation_prompt(version)


def test_invalid_prompt_fails_before_database_creation(tmp_path):
    root = tmp_path / "data"
    with pytest.raises(ValueError, match="unknown investigation prompt"):
        create_app(Config(data_dir=root, prompt_version="v999999"))
    assert not root.exists()


def test_all_packaged_prompt_versions_match_their_manifests():
    root = files("robotops.resources").joinpath("prompts", "investigation")
    for directory in root.iterdir():
        if directory.is_dir():
            metadata = prompts.load_investigation_prompt(directory.name).metadata
            assert metadata["version"] == f"investigation/{directory.name}"
