"""
    真实接入问题的离线回归：使用官方 ChatDeepSeek、SDK 和 LangGraph，不访问供应商网络。
"""

import asyncio
import json

import httpx
import pytest

from robotops.agent.client import DeepSeekClient, ModelError
from robotops.agent.context import ContextSnapshots
from robotops.agent.workflow import investigate
from robotops.config import Config
from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import Store


def test_model_branch_deadline_cancels_in_flight_request():
    """
        验证官方 SDK 的异步请求尊重整个分支剩余时间，且超时后关闭传输。

        Returns:
            None: 传输替身收到取消，超时没有伪造用量。
    """
    cancelled = []

    async def receive(_request):
        """
            模拟供应商长时间没有返回，记录本地请求是否被取消。

            Args:
                _request: SDK 构造的请求，测试无需读取其内容。

            Returns:
                httpx.Response: 模拟正常等待结束后的响应，测试中不会等到。
        """
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return httpx.Response(500)

    model = DeepSeekClient(Config(api_key="fixture"), transport=httpx.MockTransport(receive))
    with pytest.raises(ModelError, match="branch deadline") as error:
        model.complete([{"role": "user", "content": "fixture"}], [], timeout_seconds=0.5)
    assert cancelled == [True]
    assert error.value.usage == {}


@pytest.mark.parametrize("arguments", ["{}", "not-json", '{"shell":"disallowed"}'])
def test_langgraph_accepts_provider_metadata_without_relaxing_tools(tmp_path, arguments):
    """
        验证 index 不再阻断调查，同时保留坏 JSON 的修正反馈与工具参数白名单。

        Args:
            tmp_path: pytest 隔离目录，用于当前运行、证据、轨迹和请求快照。
            arguments: 合法空参数、非法 JSON 或禁止字段，分别验证成功与拒绝路径。

        Returns:
            None: 完整两轮调查通过，元数据被过滤，非法工具输入没有产生观测证据。
    """
    store = Store(tmp_path)
    store.register(ApplicationContract(app_id="example", components=["worker"]))
    incident = store.create_incident(IncidentCreate(app_id="example", symptom="没有输出"))
    requests = []

    def handler(request):
        """
            模拟供应商附带 index 的首轮调用，再根据真实工具结果返回升级报告。

            Args:
                request: 官方 SDK 经 MockTransport 发出的请求，包含本轮规范消息。

            Returns:
                httpx.Response: 工具请求或最终报告，均带固定的测试用量。
        """
        body = json.loads(request.content)
        assert "max_tokens" not in body
        requests.append(body)
        if len(requests) == 1:
            message = {
                "role": "assistant",
                "content": None,
                "reasoning_content": "private-provider-field",
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "call-contract",
                        "type": "function",
                        "function": {"name": "get_application_contract", "arguments": arguments},
                    }
                ],
            }
        else:
            result = json.loads(body["messages"][-1]["content"])
            # 验证“移除协议 index”不等于放宽函数参数；无效 JSON 也不能被框架静默丢弃。
            assert result["status"] == ("ok" if arguments == "{}" else "error")
            refs = [result["evidence_id"]] if arguments == "{}" else []
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "status": "needs_human",
                        "summary": "协议回归，不测量模型诊断能力。",
                        "recommended_action": {
                            "kind": "manual_review",
                            "reason": "需要更多信息",
                            "evidence_ids": refs,
                        },
                    }
                ),
            }
        return httpx.Response(
            200,
            json={
                "model": "fixture-model",
                "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
                "usage": {
                    "prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10,
                    "prompt_cache_hit_tokens": 2, "prompt_cache_miss_tokens": 5,
                },
            },
        )

    model = DeepSeekClient(
        Config(data_dir=tmp_path, api_key="test-only"), httpx.MockTransport(handler)
    )
    run = investigate(store, None, incident["id"], model.complete, "fixture-model")
    assert run["state"] == "COMPLETED"
    assert len(requests) == 2
    trace = store.trace(run["id"])
    first = next(event for event in trace if event["type"] == "model")
    assert "index" not in first["message"]["tool_calls"][0]
    assert "private-provider-field" not in json.dumps(trace)
    assert first["usage"] == {
        "prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10,
        "prompt_cache_hit_tokens": 2, "prompt_cache_miss_tokens": 5,
    }
    assert bool(store.evidence_ids(run["id"])) == (arguments == "{}")
    snapshots = ContextSnapshots(store, run["id"])
    assert len(snapshots.list()) == 2
    for item in snapshots.list():
        snapshots.get(item["sequence"])


def test_model_network_ignores_unsupported_environment_proxy_by_default(monkeypatch):
    """
        验证默认模型网络策略不受不支持的系统代理影响，也不改写进程的代理变量。

        Args:
            monkeypatch: pytest 的环境替换工具；测试代理为不存在的本地占位地址。

        Returns:
            None: 默认策略成功调用离线传输，显式继承代理时报告整理后的模型错误。
    """
    for key in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.setenv(key, "socks://127.0.0.1:1")
    for key in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(key, raising=False)
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "OK"}}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3},
            },
        )
    )
    result = DeepSeekClient(Config(api_key="test-only"), transport).complete(
        [{"role": "user", "content": "ping"}], []
    )
    assert result["message"]["content"] == "OK"
    # 不覆盖全局环境：后端其他服务可能仍需要代理，模型的直连策略只能作用于自己的连接。
    import os

    assert os.environ["ALL_PROXY"] == "socks://127.0.0.1:1"
    with pytest.raises(ModelError) as caught:
        DeepSeekClient(Config(api_key="test-only", model_trust_env=True)).complete([], [])
    assert "socks://" not in str(caught.value)
    assert "test-only" not in str(caught.value)
    assert caught.value.usage == {}


def test_model_http_error_is_not_retried_or_exposed():
    """
        验证供应商失败正文不进入公开异常，且 SDK 不隐式重试造成无法逐次核对的调用。

        Args:
            无显式参数。

        Returns:
            None: 恰好一次离线请求，异常只包含整理后的错误及未知用量。
    """
    requests = []

    def handler(request):
        """
            记录请求次数并返回带敏感占位正文的服务端错误。

            Args:
                request: 官方 SDK 发送的本次请求，认证仅为测试占位。

            Returns:
                httpx.Response: 应被客户端整理的 500 错误。
        """
        requests.append(request)
        return httpx.Response(500, json={"error": {"message": "private-response-secret"}})

    client = DeepSeekClient(Config(api_key="test-only"), httpx.MockTransport(handler))
    with pytest.raises(ModelError) as caught:
        client.complete([{"role": "user", "content": "ping"}], [])
    assert len(requests) == 1
    assert "private-response-secret" not in str(caught.value)
    assert "test-only" not in str(caught.value)
    assert caught.value.usage == {}
