"""模型服务的请求/响应协议与同步 HTTP 客户端。"""

import httpx

from ..config import Config
from ..models import Model


class ModelError(RuntimeError):
    """模型调用、调用协议或最终报告校验失败，可向 API 返回经过整理的错误信息。"""

    def __init__(self, message: str, usage=None):
        super().__init__(message)
        # 仅保留计费计数，不把响应原文/供应商内部字段挂在可公开的异常上。
        self.usage = {
            key: value
            for key, value in (usage if isinstance(usage, dict) else {}).items()
            if key in {"prompt_tokens", "completion_tokens", "total_tokens"}
            and isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
        }


class FunctionCall(Model):
    """模型请求的函数名和原始 JSON 参数串；参数解析后还要经过工具自己的 Schema。"""

    name: str
    arguments: str


class ToolCall(Model):
    """一次工具调用的协议外壳，id 用于将后续工具结果关联到同一请求。"""

    id: str
    type: str
    function: FunctionCall


class AssistantMessage(Model):
    """只接受调查循环使用的响应字段，最终文本和工具请求在后续节点分别处理。"""

    role: str
    content: str | None = None
    tool_calls: list[ToolCall] | None = None


class DeepSeekClient:
    """同步模型客户端，只负责 HTTP 协议适配，不执行工具或判断诊断是否正确。

    config 提供服务地址、模型名、网络超时和本地密钥。
    transport 允许测试注入 httpx.MockTransport，正常运行时由 httpx 发送真实请求。
    """

    def __init__(self, config: Config, transport=None):
        if not config.api_key:
            raise ModelError("Set DEEPSEEK_API_KEY to enable real model investigation")
        self.config = config
        self.transport = transport

    def complete(self, messages, tools):
        """发送完整消息历史与工具定义，返回 message、usage 和实际模型名。

        服务端 HTTP 错误、网络异常或响应结构不合法都会转为 ModelError。
        不把响应原文放进错误信息，避免供应商返回的敏感字段进入 API 和调查轨迹。
        此处只完成一次请求；后续是否继续调用由 LangGraph 调查节点决定。
        """
        usage = {}
        try:
            with httpx.Client(
                base_url=self.config.model_base_url.rstrip("/") + "/",
                timeout=self.config.model_timeout_seconds,
                transport=self.transport,
            ) as client:
                response = client.post(
                    "chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.config.api_key}",
                    },
                    json={
                        "model": self.config.model,
                        "messages": messages,
                        "tools": tools,
                        "thinking": {"type": "disabled"},
                    },
                )
            if response.is_error:
                raise ModelError(f"Model endpoint returned HTTP {response.status_code}")
            payload = response.json()
            usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
            message = payload["choices"][0]["message"]
            if not isinstance(message, dict) or message.get("role") != "assistant":
                raise ModelError("Model returned an invalid assistant message", usage)
            # 仅保留调查所需字段，供应商的内部推理字段和其他元数据不写入轨迹。
            message = {
                key: message[key] for key in ("role", "content", "tool_calls") if key in message
            }
            return {
                "message": message,
                "usage": payload.get("usage", {}),
                "model": payload.get("model", self.config.model),
            }
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError) as exc:
            raise ModelError("Model request failed or returned an invalid response", usage) from exc
