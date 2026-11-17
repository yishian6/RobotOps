"""启动时合并默认值、TOML 和环境变量；密钥仅从环境读取。"""

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class Config:
    """进程启动时确定的配置快照，创建后不再修改。

    data_dir 存放 SQLite、证据与轨迹；prompts_dir 可指定独立的模板资源目录。
    model_timeout_seconds 是单次网络请求超时，不是 Agent 总运行时长或 Token 限额。
    api_key 不进入 repr；配置对象本身不应作为普通业务数据序列化或写入轨迹。
    """

    data_dir: Path = Path("data")
    model: str = "deepseek-flash"
    model_base_url: str = "https://api.deepseek.com"
    api_key: str = field(default="", repr=False)
    model_timeout_seconds: float = 60
    prompts_dir: Path | None = None
    prompt_version: str = "v1"

    def __post_init__(self):
        """统一路径并拒绝不可用的模型配置，避免运行中才发现类型或地址错误。"""
        if not isinstance(self.prompt_version, str) or not re.fullmatch(
            r"v[1-9][0-9]*", self.prompt_version
        ):
            raise ValueError("Prompt version must be an explicit version such as v1")
        if not isinstance(self.model, str) or not isinstance(self.model_base_url, str):
            raise ValueError("Model name and base URL must be strings")
        if isinstance(self.model_timeout_seconds, bool) or not isinstance(
            self.model_timeout_seconds, (int, float)
        ):
            raise ValueError("Model request timeout must be a number")
        # frozen 数据类只在初始化时用 object.__setattr__ 规范化路径。
        object.__setattr__(self, "data_dir", self.data_dir.resolve())
        if self.prompts_dir is not None:
            object.__setattr__(self, "prompts_dir", self.prompts_dir.resolve())
        parsed = urlparse(self.model_base_url)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise ValueError("ROBOTOPS_MODEL_BASE_URL must be an HTTP(S) URL")
        # 访问 port 属性会校验非数字或越界端口，不能只检查 hostname。
        if parsed.port == 0:
            raise ValueError("Model URL port must be positive")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Model URL must not contain credentials, query or fragment")
        if not self.model.strip() or not 0 < self.model_timeout_seconds < float("inf"):
            raise ValueError("Model and a finite positive request timeout are required")

    @classmethod
    def load(cls):
        """按默认值、TOML、环境覆盖的优先顺序构造配置。

        默认查找工作目录的 config/robotops.toml，文件不存在时允许使用类默认值；
        一旦设置 ROBOTOPS_CONFIG，就要求该文件存在，避免配置路径错误被掩盖。
        TOML 仅接受列出的非敏感字段，密钥最终独立从 DEEPSEEK_API_KEY 读取。
        本函数不自动加载 .env，使用者应先通过 shell 或进程管理器导出环境变量。
        """
        values = {}
        explicit = os.getenv("ROBOTOPS_CONFIG")
        path = Path(explicit or "config/robotops.toml").resolve()
        if explicit or path.exists():
            with path.open("rb") as source:
                values = tomllib.load(source)
            allowed = {
                "data_dir",
                "model",
                "model_base_url",
                "model_timeout_seconds",
                "prompts_dir",
                "prompt_version",
            }
            if set(values) - allowed:
                raise ValueError(
                    "Unknown configuration keys: " + ", ".join(sorted(set(values) - allowed))
                )
            for name in ("data_dir", "prompts_dir"):
                if name in values:
                    if not isinstance(values[name], str) or not values[name].strip():
                        raise ValueError(f"{name} must be a non-empty path string")
                    values[name] = path.parent / values[name]

        # 环境中的路径相对启动目录；文件中的路径相对配置目录，避免换目录后误写数据。
        overrides = {
            "ROBOTOPS_DATA_DIR": ("data_dir", Path),
            "ROBOTOPS_MODEL": ("model", str),
            "ROBOTOPS_MODEL_BASE_URL": ("model_base_url", str),
            "ROBOTOPS_MODEL_TIMEOUT_SECONDS": ("model_timeout_seconds", float),
            "ROBOTOPS_PROMPTS_DIR": ("prompts_dir", Path),
            "ROBOTOPS_PROMPT_VERSION": ("prompt_version", str),
        }
        for variable, (name, convert) in overrides.items():
            if variable in os.environ:
                if convert is Path and not os.environ[variable].strip():
                    raise ValueError(f"{variable} must be a non-empty path")
                values[name] = convert(os.environ[variable])
        return cls(**values, api_key=os.getenv("DEEPSEEK_API_KEY", ""))
