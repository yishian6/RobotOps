"""pipeline 示例的应用组装：核心 API、只读观测和开发者控制路由。"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import APIRouter

from robotops.api import create_app as create_core_app
from robotops.config import Config
from robotops.models import ApplicationContract, Model
from robotops.storage import ConflictError

from .observer import PipelineObserver
from .runtime import PipelineRuntime


class FaultRequest(Model):
    """开发者可选的故障场景，仅用于本示例，不进入 Agent 工具注册表。"""

    mode: Literal[
        "normal", "input_stopped", "processing_stalled", "config_mismatch", "processor_exit"
    ]


def create_app(config: Config | None = None, complete=None, contract_file: Path | None = None):
    """显式组装演示应用；配置/模型可注入测试替身，但 worker 始终是真实进程。

    契约默认与示例一起安装，也可通过 contract_file 指定。生命周期仅由本工厂负责，
    正常关闭时清理其子进程。核心工厂无需理解演示的文件布局、场景名或进程控制。
    """
    config = config or Config.load()
    source = contract_file or Path(__file__).with_name("contract.json")
    contract = ApplicationContract.model_validate_json(source.read_text(encoding="utf-8"))
    if contract.app_id != "pipeline" or set(contract.components) != {"producer", "processor"}:
        raise ValueError("Example contract must describe the pipeline producer and processor")
    runtime = PipelineRuntime(config.data_dir)
    app = create_core_app(config, complete=complete, observer=PipelineObserver(runtime))
    app.state.runtime = runtime

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            runtime.stop()

    router = APIRouter(prefix="/demo", tags=["pipeline example"], lifespan=lifespan)
    store = app.state.store

    @router.post("/start", status_code=201)
    def start():
        """首次注册示例契约后启动组件，已有契约以数据库为准，不隐式覆盖。"""
        try:
            registered = store.contract("pipeline")
        except KeyError:
            registered = store.register(contract)
        if set(registered["components"]) != {"producer", "processor"}:
            raise ConflictError("Registered pipeline contract is incompatible with the example")
        return runtime.start()

    @router.get("")
    def status():
        return runtime.snapshot()

    @router.post("/fault")
    def fault(body: FaultRequest):
        """仅开发者显式调用；模型只持有 PipelineObserver 的观测能力。"""
        return runtime.inject(body.mode)

    @router.delete("", status_code=204)
    def stop():
        runtime.stop()

    app.include_router(router)
    return app
