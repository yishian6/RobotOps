"""
    版本化 Prompt 资源；使用包资源读取，源码安装和 wheel 安装行为一致。
"""

import hashlib
import json
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Literal

from pydantic import Field

from ..models import DiagnosticReport, Model

_JSON_REPORT_DIRECTIVE = "最终只输出符合以下 JSON Schema 的 JSON 对象，不加 Markdown，不输出内部思维链："


class PromptManifest(Model):
    """
        模板发布清单：逻辑名称、明确版本、完整模板内容指纹及本次变更说明。

        指纹用于发现意外改动，不是防篡改签名；变更审查仍依赖版本控制与评测。
    """

    name: Literal["investigation"]
    # 只接受从 v1 开始的明确十进制版本；路径、v0 或 latest 不能成为清单身份。
    version: str = Field(pattern=r"^v[1-9][0-9]*$")
    # SHA-256 的十六进制编码固定为 64 位小写字符；格式检查不等于内容已校验。
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    change_note: str = Field(min_length=1)


@dataclass(frozen=True)
class InvestigationPrompt:
    """
        一次加载后固定的模板快照，保存文本而不是仍会变化的文件路径。

        应用工厂在启动时创建此对象，/health 与所有调查共用同一个内容身份。
        metadata 每次返回新字典，外部序列化不会改变快照；修改磁盘模板仅影响新应用实例。
    """

    # 文本与清单身份一起冻结；调查期间修改模板文件不会改变已创建服务实例的指令。
    system: str
    task: str
    version: str
    sha256: str
    change_note: str

    @property
    def metadata(self) -> dict:
        """
            返回模板身份的可序列化副本，避免调用方改写启动时固定的快照。

            Args:
                无显式参数。

            Returns:
                dict: 新建的版本、SHA-256 与变更说明字典，不暴露可变模板对象。
        """
        return {"version": self.version, "sha256": self.sha256, "change_note": self.change_note}


def load_investigation_prompt(version: str = "v2", prompts_dir: Path | None = None):
    """
        读取指定版本，验证清单后返回不可变的文本快照。

        version 只允许 v1、v2 等明确编号，校验发生在构造资源路径之前。
        prompts_dir 显式指定时只读该目录，否则读取 config/ 打包后的内置资源。
        包资源接口兼容源码与 wheel；缺少清单/模板、清单结构错误或指纹不符均失败，
        不自动选用其他版本，保证配置写错时不会悄悄改变运行行为。

        Args:
            version (str): 明确版本号，如 v1/v2；不能使用路径、latest 或隐式回退。
            prompts_dir (Path | None): 可选模板根目录；为 None 时通过包资源读取内置模板。

        Returns:
            InvestigationPrompt: 清单、版本与内容指纹均通过校验的冻结快照。
    """
    # 先校验完整版本再拼接目录，确保版本只能选资源，不能携带 ../ 或绝对路径。
    if not re.fullmatch(r"v[1-9][0-9]*", version):
        raise ValueError("Invalid explicit prompt version")
    root = (
        prompts_dir if prompts_dir is not None else files("robotops.resources").joinpath("prompts")
    )
    # 显式资源目录与内置资源使用相同布局；用户指定的目录缺文件时必须失败，不能偷偷回退。
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
    # 指纹基于完整模板字典的稳定 JSON，连文件换行也计入；不能只校验文件名或去空白后的文本。
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
    incident: dict, contract: dict, prompt: InvestigationPrompt | None = None,
    *, report_tool_instruction: str | None = None,
):
    """
        将事件、应用契约和固定指令装配为模型初始消息及追溯元数据。

        incident/contract 来自后端持久化记录，作为 user 消息里的 JSON 数据传入；
        输出 Schema 从 DiagnosticReport 动态生成，和最终报告的校验模型共用定义。
        sha256 标识已选模板内容，messages_sha256 标识包含具体事件、契约和指令的实际初始消息。

        Args:
            incident (dict): 事件数据，作为不可信 JSON 数据装入用户消息。
            contract (dict): 当前应用契约，不用于拼接或执行用户提供的模板表达式。
            prompt (InvestigationPrompt | None): 已加载并固定的 Prompt 快照；为 None 时读取默认版本。
            report_tool_instruction: 可选的报告工具指令，替代内置模板的 JSON 输出约定。

        Returns:
            tuple[list[dict], dict]: 初始模型消息，以及模板身份和具体消息哈希。
    """
    prompt = prompt or load_investigation_prompt()
    # 保留用户选择的模板正文，只替换旧输出约定，避免与同轮提交工具的 Schema 重复或冲突。
    if report_tool_instruction is None:
        system = prompt.system + json.dumps(DiagnosticReport.model_json_schema(), ensure_ascii=False)
    else:
        system = prompt.system.replace(_JSON_REPORT_DIRECTIVE, "", 1).strip() + "\n" + report_tool_instruction
    messages = [
        {
            "role": "system",
            "content": system,
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
