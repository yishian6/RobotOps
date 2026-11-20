"""
    pipeline 示例的应用组装：核心 API、只读观测和开发者控制路由。
"""

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
    """
        开发者可选的故障场景，仅用于本示例，不进入 Agent 工具注册表。
    """

    mode: Literal[
        "normal", "input_stopped", "processing_stalled", "config_mismatch", "processor_exit"
    ]


def create_app(config: Config | None = None, complete=None, contract_file: Path | None = None):
    """
        显式组装演示应用；配置/模型可注入测试替身，但 worker 始终是真实进程。

        契约默认与示例一起安装，也可通过 contract_file 指定。生命周期仅由本工厂负责，
        正常关闭时清理其子进程。核心工厂无需理解演示的文件布局、场景名或进程控制。

        Args:
            config (Config | None): 启动配置快照；为 None 时通过 Config.load() 合并文件与环境变量。
            complete: 接收 messages/tools 的模型调用函数；为 None 时在实际调查时使用 DeepSeek。
            contract_file (Path | None): 可选示例契约路径；默认读取随包安装的 contract.json。

        Returns:
            FastAPI: 组装核心接口、只读适配器和 /demo 控制路由后的示例服务。
    """
    config = config or Config.load()
    source = contract_file or Path(__file__).with_name("contract.json")
    contract = ApplicationContract.model_validate_json(source.read_text(encoding="utf-8"))
    if contract.app_id != "pipeline" or set(contract.components) != {"producer", "processor"}:
        raise ValueError("Example contract must describe the pipeline producer and processor")
    # 示例工厂拥有进程控制权，核心只接收观测适配器；生命周期清理也由这里统一负责。
    runtime = PipelineRuntime(config.data_dir)
    app = create_core_app(config, complete=complete, observer=PipelineObserver(runtime))
    app.state.runtime = runtime

    @asynccontextmanager
    async def lifespan(app):
        """
            确保服务正常关闭或上下文异常退出时清理本会话拥有的子进程。

            Args:
                app: FastAPI 生命周期协议传入的应用；实际清理对象由闭包固定，不从请求选取。

            Returns:
                AsyncContextManager[None]: 供路由器管理进入/退出的异步生命周期上下文。

            Yields:
                None: 进入时让服务正常运行，退出时执行本会话的进程清理。
        """
        try:
            yield
        finally:
            runtime.stop()

    router = APIRouter(prefix="/demo", tags=["pipeline example"], lifespan=lifespan)
    store = app.state.store

    @router.post("/start", status_code=201)
    def start():
        """
            首次注册示例契约后启动组件，已有契约以数据库为准，不隐式覆盖。

            Args:
                无显式参数。

            Returns:
                dict: 两个组件就绪后的实际运行快照。
        """
        try:
            registered = store.contract("pipeline")
        except KeyError:
            registered = store.register(contract)
        # 已注册数据优先，但必须兼容此固定示例，不能覆盖用户契约或启动一组不匹配的进程。
        if set(registered["components"]) != {"producer", "processor"}:
            raise ConflictError("Registered pipeline contract is incompatible with the example")
        return runtime.start()

    @router.get("")
    def status():
        """
            只读查询本地示例的真实运行状态。

            Args:
                无显式参数。

            Returns:
                dict: 当前受管进程、实例和遥测快照。
        """
        return runtime.snapshot()

    @router.post("/fault")
    def fault(body: FaultRequest):
        """
            仅开发者显式调用；模型只持有 PipelineObserver 的观测能力。

            Args:
                body (FaultRequest): 经框架校验的示例故障模式；此参数不由 Agent 工具直接提交。

            Returns:
                dict: 已请求的故障模式及所属会话 ID。
        """
        return runtime.inject(body.mode)

    @router.delete("", status_code=204)
    def stop():
        """
            显式结束并清理本会话创建的子进程。

            Args:
                无显式参数。

            Returns:
                None: 正常清理后由路由器返回 HTTP 204 空响应。
        """
        runtime.stop()

    app.include_router(router)
    return app
