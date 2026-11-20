"""
    核心 API：应用、事件、调查与证据；具体场景的控制路由由应用组装方挂载。
"""

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import __version__
from .agent.client import DeepSeekClient, ModelError
from .agent.context import ContextSnapshots
from .agent.memory import MemoryStore
from .agent.prompt_loader import load_investigation_prompt
from .agent.supervision import (
    ADAPTIVE_PROMPT_VERSION,
    DEFAULT_API_MODE,
    INVESTIGATION_MODES,
    ROLE_PROMPT_VERSION,
    InvestigationMode,
    load_role_prompts,
)
from .agent.workflow import investigate
from .config import Config
from .models import ApplicationContract, IncidentCreate
from .observations import ObservationSource, ObservationUnavailable
from .retrieval import CaseCreate, CaseLibrary
from .storage import ConflictError, Store


def create_app(
    config: Config | None = None, complete=None, observer: ObservationSource | None = None
):
    """
        构造核心服务，只接收配置、模型调用和只读观测，不启动示例进程。

        config 缺省时由 Config.load 合并配置文件和环境变量；complete 缺省时使用真实
        DeepSeek。observer 由应用组装方传入，未接入时相关工具返回 unavailable。
        Prompt 先完成验证再创建存储，避免错误配置留下运行数据。示例的控制路由及子进程
        生命周期由 examples/pipeline/app.py 管理，核心不导入示例包。

        Args:
            config (Config | None): 启动配置快照；为 None 时通过 Config.load() 合并文件与环境变量。
            complete: 接收 messages/tools 的模型调用函数；为 None 时在实际调查时使用 DeepSeek。
            observer (ObservationSource | None): 当前应用的只读观测适配器；为 None 时环境观测返回不可用。

        Returns:
            FastAPI: 挂载核心业务接口并绑定 Store 的本地服务实例。
    """
    # 启动时先固定配置和 Prompt 身份，再创建存储，避免错误模板配置留下业务数据。
    config = config or Config.load()
    prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
    role_prompts = load_role_prompts()
    store = Store(config.data_dir)
    app = FastAPI(title="RobotOps Agent", version=__version__)
    app.state.store = store

    # 统一映射业务异常，路由本身保留正常调用流程；Pydantic 参数错误由框架返回 422。
    @app.exception_handler(KeyError)
    async def not_found(request: Request, exc: KeyError):
        """
            将业务异常转换为资源缺失的 404响应，保持路由错误格式一致。

            Args:
                request (Request): FastAPI 传入的请求对象；此处理器不读取其正文。
                exc (KeyError): 已由异常映射选中的业务异常，只序列化整理后的消息。

            Returns:
                JSONResponse: 含对应 HTTP 状态码和 detail 字段的错误响应。
        """
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    @app.exception_handler(ObservationUnavailable)
    async def conflict(request: Request, exc: ValueError):
        """
            将业务异常转换为状态冲突/观测不可用的 409响应，保持路由错误格式一致。

            Args:
                request (Request): FastAPI 传入的请求对象；此处理器不读取其正文。
                exc (ValueError): 已由异常映射选中的业务异常，只序列化整理后的消息。

            Returns:
                JSONResponse: 含对应 HTTP 状态码和 detail 字段的错误响应。
        """
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(ModelError)
    async def model_unavailable(request: Request, exc: ModelError):
        """
            将业务异常转换为模型配置或服务不可用的 503响应，保持路由错误格式一致。

            Args:
                request (Request): FastAPI 传入的请求对象；此处理器不读取其正文。
                exc (ModelError): 已由异常映射选中的业务异常，只序列化整理后的消息。

            Returns:
                JSONResponse: 含对应 HTTP 状态码和 detail 字段的错误响应。
        """
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.exception_handler(ValueError)
    async def invalid_request(request: Request, exc: ValueError):
        """
            将业务异常转换为业务参数无效的 400响应，保持路由错误格式一致。

            Args:
                request (Request): FastAPI 传入的请求对象；此处理器不读取其正文。
                exc (ValueError): 已由异常映射选中的业务异常，只序列化整理后的消息。

            Returns:
                JSONResponse: 含对应 HTTP 状态码和 detail 字段的错误响应。
        """
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.get("/health")
    def health():
        """
            返回服务配置摘要；不探测实际模型可用性，也不输出 API 密钥。

            Args:
                无显式参数。

            Returns:
                dict: 服务版本、模型是否配置、Prompt 身份及上下文策略；不包含密钥。
        """
        return {
            "status": "ok",
            "stage": "G1",
            "version": __version__,
            "model": config.model,
            "model_configured": bool(config.api_key),
            "prompt": prompt.metadata,
            "context_strategy": config.context_strategy,
            "investigation_modes": list(INVESTIGATION_MODES),
            "default_investigation_mode": DEFAULT_API_MODE,
            "supervision_prompt": {"version": ROLE_PROMPT_VERSION, "sha256": role_prompts.sha256},
            "adaptive_prompt": {"version": ADAPTIVE_PROMPT_VERSION, "sha256": role_prompts.adaptive_sha256},
        }

    @app.post("/applications", status_code=201)
    def register(contract: ApplicationContract):
        """
            注册应用运行约定，重复 app_id 作为冲突处理，避免意外覆盖。

            Args:
                contract (ApplicationContract): 由请求体解析并校验的应用契约。

            Returns:
                dict: 已持久化的应用契约，路由成功状态为 201。
        """
        return store.register(contract)

    @app.get("/applications/{app_id}")
    def application(app_id: str):
        """
            按应用 ID 查询运行契约。

            Args:
                app_id (str): 已注册应用的唯一标识，不是进程名或任意文件路径。

            Returns:
                dict: 对应应用的运行契约。
        """
        return store.contract(app_id)

    @app.post("/cases", status_code=201)
    def add_case(body: CaseCreate):
        """
            本地管理入口；调用者应先完成外部验证，未填验证时间的草稿不会参与检索。

            Args:
                body (CaseCreate): 由请求体解析的案例；外部验证声明与来源由调用者负责。

            Returns:
                dict: 含生成 ID 的完整案例，路由成功状态为 201。
        """
        return CaseLibrary(store).add(body)

    @app.get("/cases/{case_id}")
    def case(case_id: str):
        """
            按案例 ID 查询来源与适用条件。

            Args:
                case_id (str): 案例库生成的唯一 ID，可指向启用、草稿或已撤回的记录。

            Returns:
                dict: 案例正文，含草稿、过期或撤回状态。
        """
        return CaseLibrary(store).get(case_id)

    @app.delete("/cases/{case_id}")
    def disable_case(case_id: str):
        """
            撤回案例：保留可审查来源，后续检索不再返回，已有证据不改写。

            Args:
                case_id (str): 案例库生成的唯一 ID，可指向启用、草稿或已撤回的记录。

            Returns:
                dict: 已撤回但仍保留来源的案例正文。
        """
        return CaseLibrary(store).disable(case_id)

    @app.get("/applications/{app_id}/cases")
    def search_cases(
        app_id: str,
        query: str = Query(min_length=1, max_length=2000),
        limit: int = Query(default=5, ge=1, le=20),
    ):
        """
            按当前契约搜索可用案例，不因空结果扩大到其他版本。

            Args:
                app_id (str): 已注册应用的唯一标识，不是进程名或任意文件路径。
                query (str): 1–2000 字符的自然检索文本；由框架先校验长度。
                limit (int): 最大返回条数，由框架约束为 1–20，默认 5。

            Returns:
                list[dict]: 当前应用/版本适用的已验证案例，无匹配时为空。
        """
        return CaseLibrary(store).search(store.contract(app_id), query, limit=limit)

    @app.post("/incidents", status_code=201)
    def create_incident(body: IncidentCreate):
        """
            登记待调查症状，不在接入阶段调用模型或执行恢复。

            Args:
                body (IncidentCreate): 已校验的应用、症状及来源请求体。

            Returns:
                dict: 新建的 OPEN 事件记录，成功状态为 201。
        """
        return store.create_incident(body)

    @app.get("/incidents")
    def incidents():
        """
            列出本地已登记事件，便于查看待调查与已调查状态。

            Args:
                无显式参数。

            Returns:
                list[dict]: 按创建时间排列的全部事件记录。
        """
        return store.incidents()

    @app.get("/incidents/{incident_id}")
    def incident(incident_id: str):
        """
            按事件 ID 查询详情。

            Args:
                incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。

            Returns:
                dict: 事件详情与业务状态。
        """
        return store.incident(incident_id)

    @app.post("/incidents/{incident_id}/investigate")
    def investigate_incident(incident_id: str, mode: InvestigationMode = DEFAULT_API_MODE):
        """
            同步执行一次只读调查；缺少模型密钥会在创建运行记录前返回错误。

            同事件已有 RUNNING 调查时由存储层拒绝重复启动；图执行失败也会返回运行记录，
            调用者需查看其中的 state/error，不能把 HTTP 响应成功等同于诊断成功。

            Args:
                incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。
                mode (InvestigationMode): 默认按需委派，可显式选择单 Agent 或 M1 监督对照。

            Returns:
                dict: 调查运行记录；必须检查 state/error，HTTP 成功不等同于诊断成功。
        """
        # 沿用同步入口，调用方可对同一事件选择一种调查方式。
        store.incident(incident_id)
        callback = complete or DeepSeekClient(config).complete
        return investigate(
            store,
            observer,
            incident_id,
            callback,
            config.model,
            prompt,
            config.context_strategy,
            mode=mode,
            role_prompts=role_prompts,
        )

    @app.get("/runs/{run_id}")
    def investigation(run_id: str):
        """
            查询单次调查的持久化结果。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                dict: 调查运行、报告及错误信息。
        """
        return store.run(run_id)

    @app.get("/runs/{run_id}/trace")
    def trace(run_id: str):
        """
            读取指定调查的模型与工具调用轨迹。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                list[dict]: 当前运行的有序轨迹，尚无文件时为空列表。
        """
        return store.trace(run_id)

    @app.get("/runs/{run_id}/evidence/{evidence_id}")
    def evidence(run_id: str, evidence_id: str):
        """
            查询原始证据，同时执行运行范围与完整性校验。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                evidence_id (str): 平台生成的证据 ID；需要同时校验所属调查运行。

            Returns:
                dict: 所属运行的证据元数据和校验后的原文。
        """
        return store.read_evidence(run_id, evidence_id)

    @app.get("/runs/{run_id}/memory")
    def memory(run_id: str):
        """
            查看当前调查使用的工作记忆。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                dict: 最新记忆版本、结构化记忆及保护引用；无记忆时为版本 0。
        """
        return MemoryStore(store, run_id).current()

    @app.get("/runs/{run_id}/contexts")
    def contexts(run_id: str):
        """
            列出上下文快照索引，避免一次传输全部历史请求正文。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                list[dict]: 按序号排列的快照索引，只含 sequence 和 sha256。
        """
        return ContextSnapshots(store, run_id).list()

    @app.get("/runs/{run_id}/contexts/{sequence}")
    def context(run_id: str, sequence: int):
        """
            读取并校验某轮实际发送的上下文快照。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                sequence (int): 当前运行内从 1 开始递增的上下文快照序号。

            Returns:
                dict: 精确模型输入、工具 Schema、记忆及装配清单。
        """
        return ContextSnapshots(store, run_id).get(sequence)

    @app.get("/runs/{run_id}/usage")
    def usage(run_id: str):
        """
            累计实际返回的用量；缺失项计数单列，不把缺失当免费或推算货币费用。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                dict: 实报输入/输出及缓存用量、各项缺失计数和请求数量。
        """
        events = [item for item in store.trace(run_id) if item["type"] in {"model", "model_error"}]
        # 用量总和与缺失次数分开统计；部分供应商只报告部分字段，不能把空缺补成零费用。
        cache_fields = ("prompt_cache_hit_tokens", "prompt_cache_miss_tokens")
        counts = dict.fromkeys(("prompt_tokens", "completion_tokens", *cache_fields), 0)
        missing = {key: 0 for key in counts}
        for event in events:
            data = event.get("usage") or {}
            for key in counts:
                value = data.get(key)
                # 缺失、负数和布尔值都不算已报告用量，避免把错误返回当成完整计费数据。
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    counts[key] += value
                else:
                    missing[key] += 1
        requests = len(ContextSnapshots(store, run_id).list())
        # 兼容没有逐轮快照的旧运行；同时让已保存请求但没收到可记录响应的尝试显式留为未知。
        unreported = max(0, requests - len(events))
        for key in missing:
            missing[key] += unreported
        return {
            "recorded_attempts": len(events),
            "reported_tokens": {key: counts[key] for key in counts if key not in cache_fields},
            "missing_usage": {key: missing[key] for key in missing if key not in cache_fields},
            "reported_cache_tokens": {
                key: counts[key] if missing[key] < len(events) + unreported else None
                for key in cache_fields
            },
            "missing_cache_usage": {key: missing[key] for key in cache_fields},
            "context_requests": requests,
            "unreported_attempts": unreported,
            "includes_summary_calls": False,
        }

    return app
