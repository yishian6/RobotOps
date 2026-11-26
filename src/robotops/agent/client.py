"""
    LangGraph 调查使用的 ChatDeepSeek 模型适配：统一协议、隔离网络配置并保留已报告用量。
"""

import asyncio
import json

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import LLMResult
from langchain_deepseek import ChatDeepSeek
from openai import BadRequestError, DefaultAsyncHttpxClient, DefaultHttpxClient

from ..config import Config
from ..models import Model


def _reported_usage(usage) -> dict[str, int]:
    """
        筛选供应商实报计数，统一成功响应、回调和异常的用量口径。

        Args:
            usage: 供应商用量字典或未知值；不接收或保留响应正文。

        Returns:
            dict[str, int]: 白名单内的非布尔、非负整数；缺失项不补零。
    """
    # bool 是 int 的子类，需要单独排除；未知字段和负数不作为已报告计费数据。
    return {
        key: value
        for key, value in (usage if isinstance(usage, dict) else {}).items()
        if key in {
            "prompt_tokens", "completion_tokens", "total_tokens",
            "prompt_cache_hit_tokens", "prompt_cache_miss_tokens",
        }
        and isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    }


class ModelError(RuntimeError):
    """
        模型调用、调用协议或最终报告校验失败，可向 API 返回经过整理的错误信息。
    """

    def __init__(self, message: str, usage=None):
        """
            构造可对外返回的模型错误，并只保留有效的计费计数。

            Args:
                message (str): 调用方提供的已整理错误消息，不应包含密钥或响应原文。
                usage: 可选供应商用量字典；只保留白名单内的非布尔、非负整数计数。

            Returns:
                None: 初始化异常消息及过滤后的用量字典。
        """
        super().__init__(message)
        # 异常可由 API 返回，不能把响应原文或供应商内部字段挂在公开对象上。
        self.usage = _reported_usage(usage)


class ContextLengthError(ModelError):
    """The provider explicitly rejected the input due to context length."""


def _context_length_rejected(error: Exception) -> bool:
    if not isinstance(error, BadRequestError):
        return False
    if error.code in ("context_length_exceeded", "context_window_exceeded"):
        return True
    message = error.message.lower()
    return "maximum context length" in message and ("tokens" in message or "exceed" in message)


class FunctionCall(Model):
    """
        模型请求的函数名和原始 JSON 参数串；参数解析后还要经过工具自己的 Schema。
    """

    name: str
    # 协议要求参数是 JSON 字符串；先保留原串，工作流解析后再按具体工具模型校验。
    arguments: str


class ToolCall(Model):
    """
        一次工具调用的协议外壳，id 用于将后续工具结果关联到同一请求。
    """

    id: str
    type: str
    function: FunctionCall


class AssistantMessage(Model):
    """
        只接受调查循环使用的响应字段，最终文本和工具请求在后续节点分别处理。
    """

    role: str
    content: str | None = None
    # None/空列表都表示没有请求工具；content 随后必须通过最终诊断报告校验。
    tool_calls: list[ToolCall] | None = None


class _UsageRecorder(BaseCallbackHandler):
    """
        在框架解析结果后保留供应商原始计数，避免空 generations 等协议错误丢失已发生用量。

        回调只保存白名单内的计数，不保存消息、请求头或供应商其他字段。
    """

    def __init__(self):
        """
            创建仅供一次模型请求使用的用量记录器。

            Args:
                无显式参数。

            Returns:
                None: 初始化未知用量为空字典，不推断为零费用。
        """
        self.usage = {}

    def on_llm_end(self, response: LLMResult, **kwargs):
        """
            从框架完成事件中提取已报告计数；消息解析后续失败时仍可记录。

            Args:
                response (LLMResult): 框架生成结果，llm_output 可包含供应商 token_usage。
                **kwargs: LangChain 回调协议附带参数；本记录器不读取或存储。

            Returns:
                None: 更新当前请求的计数快照，不保留完整结果。
        """
        # 复用公开异常已有的计数筛选，确保成功与失败路径不引入不同的用量口径。
        self.usage = _reported_usage((response.llm_output or {}).get("token_usage"))


class DeepSeekClient:
    """
        通过官方 ChatDeepSeek 的 bind_tools/ainvoke 接入现有 LangGraph 调查节点。

        对外保持同步 complete 协议；内部异步调用便于完整关闭 SDK 创建的两类网络客户端。
        请求参数、工具绑定和供应商协议由集成组件维护；本类只做项目边界转换与错误整理。
    """

    def __init__(self, config: Config, transport=None):
        """
            固定模型配置，缺少密钥时在创建调查运行前失败。

            Args:
                config (Config): 已校验的模型配置；model_trust_env 控制是否继承网络环境变量。
                transport: 可选 SDK 传输替身，仅用于离线协议测试，不参与业务工具分派。

            Returns:
                None: 保存依赖，不发起请求或创建长寿命网络连接。
        """
        if not config.api_key:
            raise ModelError("Set DEEPSEEK_API_KEY to enable real model investigation")
        self.config = config
        self.transport = transport

    def complete(self, messages, tools, *, timeout_seconds: float | None = None):
        """
            同步执行一次框架模型调用，返回现有调查节点使用的消息、用量和模型身份。

            此入口由 LangGraph 同步节点调用，异步应用应在线程中调用同步调查流程。
            不在这里循环调查或自动重试，避免一次业务请求产生未单独记录的重复模型费用。

            Args:
                messages: 装配后的 system/user/assistant/tool 消息；工具回复必须与调用 ID 配对。
                tools: 当前网关生成的函数 Schema，直接交给框架绑定，不放宽业务参数校验。
                timeout_seconds: 子任务剩余的墙钟时长；为 None 时沿用单次网络超时。

            Returns:
                dict: 含白名单 message、已报告 usage 和实际模型名称的响应。
        """
        request = self._acomplete(messages, tools)
        if timeout_seconds is None:
            return asyncio.run(request)
        try:
            # 取消异步请求时仍会退出 SDK 的连接上下文，未返回的计费用量保持未知。
            return asyncio.run(asyncio.wait_for(request, timeout_seconds))
        except TimeoutError as exc:
            raise ModelError("Model request exceeded the branch deadline") from exc

    async def _acomplete(self, messages, tools):
        """
            管理 SDK 连接生命周期，通过 ChatDeepSeek 调用并整理项目协议。

            Args:
                messages: 完整的本轮发送视图，不包含 API 密钥或私有故障控制数据。
                tools: 可调用工具定义列表；空列表表示只进行普通文本调用。

            Returns:
                dict: 可持久化的模型响应，不含 reasoning_content、请求头或供应商私有字段。
        """
        recorder = _UsageRecorder()
        try:
            # 框架底层仍使用 SDK 网络库。显式控制 trust_env，不修改全局代理变量或关闭 TLS 校验。
            # ChatDeepSeek 会同时初始化同步/异步 SDK，因此两类客户端都必须使用相同网络策略并关闭。
            with DefaultHttpxClient(
                trust_env=self.config.model_trust_env, transport=self.transport
            ) as sync_http:
                async with DefaultAsyncHttpxClient(
                    trust_env=self.config.model_trust_env, transport=self.transport
                ) as async_http:
                    model = ChatDeepSeek(
                        model=self.config.model,
                        api_key=self.config.api_key,
                        api_base=self.config.model_base_url,
                        timeout=self.config.model_timeout_seconds,
                        max_tokens=self.config.model_output_tokens,
                        max_retries=0,
                        cache=False,
                        http_client=sync_http,
                        http_async_client=async_http,
                        extra_body={"thinking": {"type": "disabled"}},
                    )
                    bound = model.bind_tools(tools) if tools else model
                    # 字典消息转换器会立即解析 arguments，遇到上轮坏 JSON 就拒绝整个请求。
                    # AIMessage 则将坏参数保留为 invalid_tool_calls，让下一轮仍能收到网关的修正反馈。
                    history = [
                        AIMessage(
                            content=item.get("content") or "",
                            additional_kwargs={"tool_calls": item["tool_calls"]},
                        )
                        if item["role"] == "assistant" and item.get("tool_calls")
                        else item
                        for item in messages
                    ]
                    reply = await bound.ainvoke(history, config={"callbacks": [recorder]})
            # 仅文本助手消息可进入当前调查协议；其他类型不能被包装成合法的诊断报告。
            if not isinstance(reply, AIMessage) or not isinstance(reply.content, str):
                raise ModelError("Model returned an invalid assistant message", recorder.usage)
            message = {"role": "assistant", "content": reply.content}
            # 框架已把 index 等供应商元数据归一化掉；这里只转换调查历史沿用的参数字符串。
            # 无效调用单列保留给网关反馈错误，不能静默丢弃后把空 content 当作最终报告。
            calls = [
                {
                    "id": call["id"],
                    "type": "function",
                    "function": {
                        "name": call["name"],
                        "arguments": json.dumps(call["args"], ensure_ascii=False, allow_nan=False),
                    },
                }
                for call in reply.tool_calls
            ]
            calls.extend(
                {
                    "id": call["id"],
                    "type": "function",
                    "function": {"name": call["name"], "arguments": call["args"]},
                }
                for call in reply.invalid_tool_calls
            )
            if calls:
                message["tool_calls"] = calls
            # 不用框架补零后的 usage_metadata 冒充供应商实报值；缺失计数继续留为未知。
            return {
                "message": AssistantMessage.model_validate(message).model_dump(exclude_none=True),
                "usage": recorder.usage,
                "model": reply.response_metadata.get("model_name") or self.config.model,
            }
        except ModelError:
            raise
        except Exception as exc:
            # SDK/框架异常可能携带响应正文、代理 URL 或请求头，只保留通用错误类别与有效计数。
            if _context_length_rejected(exc):
                raise ContextLengthError("Model input exceeded the provider context window", recorder.usage) from exc
            raise ModelError(
                "Model request failed or returned an invalid response", recorder.usage
            ) from exc
