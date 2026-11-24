"""
    启动时合并默认值、TOML 和环境变量；密钥仅从环境读取。
"""

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class Config:
    """
        进程启动时确定的配置快照，创建后不再修改。

        data_dir 存放 SQLite、证据与轨迹；prompts_dir 可指定独立的模板资源目录。
        model_timeout_seconds 是单次网络请求超时，不是 Agent 总运行时长或 Token 限额。
        api_key 不进入 repr；配置对象本身不应作为普通业务数据序列化或写入轨迹。
    """

    # 数据路径在构造时转绝对路径，后续工作目录变化不应改变证据与数据库位置。
    data_dir: Path = Path("data")
    model: str = "deepseek-flash"
    model_base_url: str = "https://api.deepseek.com"
    # 密钥独立于可提交的 TOML，只允许由运行环境注入，repr 也不能显示它。
    api_key: str = field(default="", repr=False)
    model_timeout_seconds: float = 60
    # 分支等待独立于单次 HTTP 超时和整次调查，给一次取证及一次发现提交留下时间。
    branch_timeout_seconds: float = 120
    monitor_interval_seconds: float = 5
    # 模型连接默认直连，避免桌面/终端代理隐式改变后端网络；需要代理时再显式继承环境配置。
    model_trust_env: bool = False
    prompts_dir: Path | None = None
    # 版本固定指令身份；上下文策略只改变历史呈现方式，不改变证据原文与调查权限。
    prompt_version: str = "v2"
    context_strategy: str = "evidence"

    def __post_init__(self):
        """
            统一路径并拒绝不可用的模型配置，避免运行中才发现类型或地址错误。

            Args:
                无显式参数。

            Returns:
                None: 在冻结配置构造时规范化路径并检查值。
        """
        if not isinstance(self.context_strategy, str) or self.context_strategy not in {
            "full",
            "rolling",
            "evidence",
        }:
            raise ValueError("Context strategy must be full, rolling or evidence")
        # v 后第一位必须为 1–9，其后才允许 0–9；明确版本避免路径穿越、v0 与含糊别名。
        if not isinstance(self.prompt_version, str) or not re.fullmatch(
            r"v[1-9][0-9]*", self.prompt_version
        ):
            raise ValueError("Prompt version must be an explicit version such as v1")
        if not isinstance(self.model, str) or not isinstance(self.model_base_url, str):
            raise ValueError("Model name and base URL must be strings")
        if not isinstance(self.model_trust_env, bool):
            raise ValueError("Model trust_env must be a boolean")
        # Python 的 bool 是 int 子类，需单独排除，避免 true/false 被误解释为秒数。
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
        if type(self.branch_timeout_seconds) not in (int, float) or not 0 < self.branch_timeout_seconds < float("inf"):
            raise ValueError("Branch timeout must be finite and positive")
        if type(self.monitor_interval_seconds) not in (int, float) or not 0 < self.monitor_interval_seconds < float("inf"):
            raise ValueError("Monitor interval must be finite and positive")

    @classmethod
    def load(cls):
        """
            按默认值、TOML、环境覆盖的优先顺序构造配置。

            默认查找工作目录的 config/robotops.toml，文件不存在时允许使用类默认值；
            一旦设置 ROBOTOPS_CONFIG，就要求该文件存在，避免配置路径错误被掩盖。
            TOML 仅接受列出的非敏感字段，密钥最终独立从 DEEPSEEK_API_KEY 读取。
            本函数不自动加载 .env，使用者应先通过 shell 或进程管理器导出环境变量。

            Args:
                无显式参数。

            Returns:
                Config: 已合并且通过构造校验的不可变配置快照。
        """
        values = {}
        explicit = os.getenv("ROBOTOPS_CONFIG")
        path = Path(explicit or "config/robotops.toml").resolve()
        if explicit or path.exists():
            with path.open("rb") as source:
                values = tomllib.load(source)
            # 使用显式白名单，阻止 TOML 携带 api_key 或意外覆盖内部构造参数。
            allowed = {
                "data_dir",
                "model",
                "model_base_url",
                "model_timeout_seconds",
                "branch_timeout_seconds",
                "model_trust_env",
                "monitor_interval_seconds",
                "prompts_dir",
                "prompt_version",
                "context_strategy",
            }
            # 配置键拼错必须启动失败，否则看似设置成功却仍使用默认值，故障调查将无法复现。
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
            "ROBOTOPS_BRANCH_TIMEOUT_SECONDS": ("branch_timeout_seconds", float),
            "ROBOTOPS_MONITOR_INTERVAL_SECONDS": ("monitor_interval_seconds", float),
            "ROBOTOPS_PROMPTS_DIR": ("prompts_dir", Path),
            "ROBOTOPS_PROMPT_VERSION": ("prompt_version", str),
            "ROBOTOPS_CONTEXT_STRATEGY": ("context_strategy", str),
        }
        for variable, (name, convert) in overrides.items():
            if variable in os.environ:
                # Path(空字符串) 会指向当前目录，提前拒绝空白值可避免意外写入错误的数据位置。
                if convert is Path and not os.environ[variable].strip():
                    raise ValueError(f"{variable} must be a non-empty path")
                values[name] = convert(os.environ[variable])
        # 不用 bool(字符串)：bool("false") 也是 True，代理开关必须按明确字面值解析。
        if "ROBOTOPS_MODEL_TRUST_ENV" in os.environ:
            value = os.environ["ROBOTOPS_MODEL_TRUST_ENV"].strip().lower()
            if value not in {"true", "false"}:
                raise ValueError("ROBOTOPS_MODEL_TRUST_ENV must be true or false")
            values["model_trust_env"] = value == "true"
        return cls(**values, api_key=os.getenv("DEEPSEEK_API_KEY", ""))
