"""版本化 Prompt 资源；使用包资源读取，源码安装和 wheel 安装行为一致。"""

import hashlib
import json
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Literal

from pydantic import Field

from ..models import DiagnosticReport, Model


class PromptManifest(Model):
    """模板发布清单：逻辑名称、明确版本、完整模板内容指纹及本次变更说明。

    指纹用于发现意外改动，不是防篡改签名；变更审查仍依赖版本控制与评测。
    """

    name: Literal["investigation"]
    version: str = Field(pattern=r"^v[1-9][0-9]*$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    change_note: str = Field(min_length=1)


@dataclass(frozen=True)
class InvestigationPrompt:
    """一次加载后固定的模板快照，保存文本而不是仍会变化的文件路径。

    应用工厂在启动时创建此对象，/health 与所有调查共用同一个内容身份。
    metadata 每次返回新字典，外部序列化不会改变快照；修改磁盘模板仅影响新应用实例。
    """

    system: str
    task: str
    version: str
    sha256: str
    change_note: str

    @property
    def metadata(self) -> dict:
        return {"version": self.version, "sha256": self.sha256, "change_note": self.change_note}


def load_investigation_prompt(version: str = "v2", prompts_dir: Path | None = None):
    """读取指定版本，验证清单后返回不可变的文本快照。

    version 只允许 v1、v2 等明确编号，校验发生在构造资源路径之前。
    prompts_dir 显式指定时只读该目录，否则读取 config/ 打包后的内置资源。
    包资源接口兼容源码与 wheel；缺少清单/模板、清单结构错误或指纹不符均失败，
    不自动选用其他版本，保证配置写错时不会悄悄改变运行行为。
    """
    if not re.fullmatch(r"v[1-9][0-9]*", version):
        raise ValueError("Invalid explicit prompt version")
    root = (
        prompts_dir if prompts_dir is not None else files("robotops.resources").joinpath("prompts")
    )
    directory = root.joinpath("investigation", version)
    try:
        manifest = PromptManifest.model_validate_json(
            directory.joinpath("manifest.json").read_text(encoding="utf-8")
        )
        # 对完整文件内容做校验，不能无声修改同版本的实际指令。
        templates = {
            name: directory.joinpath(f"{name}.txt").read_text(encoding="utf-8")
            for name in ("system", "task")
        }
    except FileNotFoundError as exc:
        raise ValueError(f"Incomplete or unknown investigation prompt: {version}") from exc
    digest = hashlib.sha256(
        json.dumps(templates, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    if manifest.version != version or manifest.sha256 != digest:
        raise ValueError(f"Prompt manifest/content mismatch: investigation/{version}")
    return InvestigationPrompt(
        system=templates["system"],
        task=templates["task"],
        version=f"investigation/{version}",
        sha256=digest,
        change_note=manifest.change_note,
    )


def investigation_messages(
    incident: dict, contract: dict, prompt: InvestigationPrompt | None = None
):
    """将事件、应用契约和固定指令装配为模型初始消息及追溯元数据。

    incident/contract 来自后端持久化记录，作为 user 消息里的 JSON 数据传入；
    输出 Schema 从 DiagnosticReport 动态生成，和最终报告的校验模型共用定义。
    sha256 标识模板内容，messages_sha256 还包含具体事件、契约和输出 Schema，
    因此同一模板处理不同事件时可以分别识别模板版本和实际输入。
    """
    prompt = prompt or load_investigation_prompt()
    messages = [
        {
            "role": "system",
            # Schema 来自同一个 Pydantic 模型，避免模板与后端校验规则分别维护。
            "content": prompt.system
            + json.dumps(DiagnosticReport.model_json_schema(), ensure_ascii=False),
        },
        {
            "role": "user",
            # 不对日志/事件文本执行模板替换，花括号等内容保留为普通 JSON 数据。
            "content": json.dumps(
                {"incident": incident, "contract": contract, "task": prompt.task.strip()},
                ensure_ascii=False,
            ),
        },
    ]
    fingerprint = hashlib.sha256(
        json.dumps(messages, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    return messages, {
        **prompt.metadata,
        "messages_sha256": fingerprint,
    }
