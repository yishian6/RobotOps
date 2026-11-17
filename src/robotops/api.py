"""核心 API：应用、事件、调查与证据；具体场景的控制路由由应用组装方挂载。"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import __version__
from .agent.client import DeepSeekClient, ModelError
from .agent.prompt_loader import load_investigation_prompt
from .agent.workflow import investigate
from .config import Config
from .models import ApplicationContract, IncidentCreate
from .observations import ObservationSource, ObservationUnavailable
from .storage import ConflictError, Store


def create_app(
    config: Config | None = None, complete=None, observer: ObservationSource | None = None
):
    """构造核心服务，只接收配置、模型调用和只读观测，不启动示例进程。

    config 缺省时由 Config.load 合并配置文件和环境变量；complete 缺省时使用真实
    DeepSeek。observer 由应用组装方传入，未接入时相关工具返回 unavailable。
    Prompt 先完成验证再创建存储，避免错误配置留下运行数据。示例的控制路由及子进程
    生命周期由 examples/pipeline/app.py 管理，核心不导入示例包。
    """
    config = config or Config.load()
    prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
    store = Store(config.data_dir)
    app = FastAPI(title="RobotOps Agent", version=__version__)
    app.state.store = store

    # 统一映射业务异常，路由本身保留正常调用流程；Pydantic 参数错误由框架返回 422。
    @app.exception_handler(KeyError)
    async def not_found(request: Request, exc: KeyError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    @app.exception_handler(ObservationUnavailable)
    async def conflict(request: Request, exc: ValueError):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(ModelError)
    async def model_unavailable(request: Request, exc: ModelError):
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.exception_handler(ValueError)
    async def invalid_request(request: Request, exc: ValueError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.get("/health")
    def health():
        """返回服务配置摘要；不探测实际模型可用性，也不输出 API 密钥。"""
        return {
            "status": "ok",
            "stage": "G0",
            "version": __version__,
            "model": config.model,
            "model_configured": bool(config.api_key),
            "prompt": prompt.metadata,
        }

    @app.post("/applications", status_code=201)
    def register(contract: ApplicationContract):
        """注册应用运行约定，重复 app_id 作为冲突处理，避免意外覆盖。"""
        return store.register(contract)

    @app.get("/applications/{app_id}")
    def application(app_id: str):
        return store.contract(app_id)

    @app.post("/incidents", status_code=201)
    def create_incident(body: IncidentCreate):
        """登记待调查症状，不在接入阶段调用模型或执行恢复。"""
        return store.create_incident(body)

    @app.get("/incidents")
    def incidents():
        return store.incidents()

    @app.get("/incidents/{incident_id}")
    def incident(incident_id: str):
        return store.incident(incident_id)

    @app.post("/incidents/{incident_id}/investigate")
    def investigate_incident(incident_id: str):
        """同步执行一次只读调查；缺少模型密钥会在创建运行记录前返回错误。

        同事件已有 RUNNING 调查时由存储层拒绝重复启动；图执行失败也会返回运行记录，
        调用者需查看其中的 state/error，不能把 HTTP 响应成功等同于诊断成功。
        """
        # G0 尚无持久异步调度或崩溃续跑；这些能力在 G2 引入。
        store.incident(incident_id)
        callback = complete or DeepSeekClient(config).complete
        return investigate(
            store,
            observer,
            incident_id,
            callback,
            config.model,
            prompt,
        )

    @app.get("/runs/{run_id}")
    def investigation(run_id: str):
        return store.run(run_id)

    @app.get("/runs/{run_id}/trace")
    def trace(run_id: str):
        return store.trace(run_id)

    @app.get("/runs/{run_id}/evidence/{evidence_id}")
    def evidence(run_id: str, evidence_id: str):
        return store.read_evidence(run_id, evidence_id)

    return app
