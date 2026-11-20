"""
    业务持久化：SQLite 保存关联关系，独立文件保存原始证据和逐轮调查轨迹。
"""

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .models import ApplicationContract, IncidentCreate, utc_now


class ConflictError(ValueError):
    """
        应用重复注册或同事件已有活跃调查等业务冲突，由 API 映射为 409。
    """

    pass


class Store:
    """
        应用、事件、运行记录及证据索引的本地存储入口。

        root/runtime.db 保存业务数据；evidence/ 存原始 JSON，traces/ 存逐运行 JSONL。
        证据使用新 ID 独占创建并保存哈希，回读时校验归属与完整性。
        文件和数据库不是同一个事务；此实现不承诺跨资源原子提交或崩溃自动恢复。
    """

    def __init__(self, root: Path):
        """
            创建可持久化的本地业务存储。

            Args:
                root (Path): 数据根目录；内部创建 runtime.db、evidence/ 和 traces/。

            Returns:
                None: 初始化数据目录、线程锁及数据库表，不启动调查。
        """
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "evidence").mkdir(exist_ok=True)
        (self.root / "traces").mkdir(exist_ok=True)
        # 同一 Store 的轨迹追加/读取互斥，防止线程读到本实例尚未写完的一行 JSON。
        self._trace_lock = threading.Lock()
        with self.connection() as db:
            # WAL 便于本机读写并行；各表通过外键关联，活跃调查由部分唯一索引去重。
            db.execute("PRAGMA journal_mode=WAL")
            # 业务主链为 applications -> incidents -> runs，证据索引同时绑定事件和运行。
            # cases 保存可撤回正文，cases_fts 仅供词法排序；两者的业务写入由同一事务维护。
            # memories 按版本保留注记，context_snapshots 按请求序号保存精确输入，二者用途不同。
            # one_active_investigation 仅约束 RUNNING，保留同一事件的多次历史调查。
            db.executescript("""
                CREATE TABLE IF NOT EXISTS applications (
                    app_id TEXT PRIMARY KEY, contract TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY, app_id TEXT NOT NULL REFERENCES applications,
                    symptom TEXT NOT NULL, source TEXT NOT NULL,
                    state TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES incidents,
                    model TEXT NOT NULL, state TEXT NOT NULL, started_at TEXT NOT NULL,
                    finished_at TEXT, report TEXT, error TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_investigation
                    ON runs(incident_id) WHERE state = 'RUNNING';
                CREATE TABLE IF NOT EXISTS evidence (
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES incidents,
                    run_id TEXT NOT NULL REFERENCES runs, tool TEXT NOT NULL,
                    observed_at TEXT NOT NULL, sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS evidence_run ON evidence(run_id);
                CREATE TABLE IF NOT EXISTS cases (
                    id TEXT PRIMARY KEY, app_id TEXT NOT NULL, app_version TEXT NOT NULL,
                    body TEXT NOT NULL
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS cases_fts USING fts5(case_id UNINDEXED, terms);
                CREATE TABLE IF NOT EXISTS memories (
                    run_id TEXT NOT NULL REFERENCES runs, version INTEGER NOT NULL,
                    body TEXT NOT NULL, PRIMARY KEY(run_id, version)
                );
                CREATE TABLE IF NOT EXISTS context_snapshots (
                    run_id TEXT NOT NULL REFERENCES runs, sequence INTEGER NOT NULL,
                    body TEXT NOT NULL, sha256 TEXT NOT NULL,
                    PRIMARY KEY(run_id, sequence)
                );
            """)

    @contextmanager
    def connection(self):
        """
            提供一次操作的数据库连接：正常提交、异常回滚，最后始终关闭连接。

            每次独立连接，避免 FastAPI 同步处理线程共享 sqlite3 连接。
            外键和同步级别是连接级设置，因此不能只在数据库首次初始化时设置。

            Args:
                无显式参数。

            Returns:
                ContextManager[sqlite3.Connection]: 正常退出提交、异常退出回滚的短连接上下文。

            Yields:
                sqlite3.Connection: 当前操作独占使用的连接，在上下文退出时始终关闭。
        """
        db = sqlite3.connect(self.root / "runtime.db", timeout=5)
        # 使用字段名读取索引，减少表字段位置变化时把归属或状态读错的风险。
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def register(self, contract: ApplicationContract):
        """
            首次注册应用契约并返回 JSON 兼容字典；不隐式覆盖已有应用定义。

            Args:
                contract (ApplicationContract): 已通过 Pydantic 校验的应用契约，含组件与依赖定义。

            Returns:
                dict: 注册后的应用契约，已转换为 JSON 兼容字段。
        """
        with self.connection() as db:
            try:
                db.execute(
                    "INSERT INTO applications VALUES (?, ?)",
                    (contract.app_id, contract.model_dump_json()),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("Application already registered") from exc
        return contract.model_dump(mode="json")

    def contract(self, app_id: str):
        """
            按应用 ID 返回已保存契约；缺失抛 KeyError，供 API 映射为 404。

            Args:
                app_id (str): 已注册应用的唯一标识，不是进程名或任意文件路径。

            Returns:
                dict: 持久化的应用契约正文。
        """
        with self.connection() as db:
            row = db.execute(
                "SELECT contract FROM applications WHERE app_id=?", (app_id,)
            ).fetchone()
        if row is None:
            raise KeyError("Application not found")
        return json.loads(row[0])

    def create_incident(self, request: IncidentCreate):
        """
            确认应用存在后创建 OPEN 事件；事件用于描述症状，不直接认定故障根因。

            Args:
                request (IncidentCreate): 已校验的事件接入模型，指定应用、症状和来源。

            Returns:
                dict: 新建事件的 ID、症状、来源、时间及 OPEN 状态。
        """
        self.contract(request.app_id)
        incident_id = f"inc-{uuid4().hex}"
        with self.connection() as db:
            db.execute(
                "INSERT INTO incidents VALUES (?, ?, ?, ?, 'OPEN', ?)",
                (incident_id, request.app_id, request.symptom, request.source, utc_now()),
            )
        return self.incident(incident_id)

    def incident(self, incident_id: str):
        """
            读取事件记录；找不到时抛 KeyError，不返回含义不明的空对象。

            Args:
                incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。

            Returns:
                dict: 事件详情及当前业务状态。
        """
        with self.connection() as db:
            row = db.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if row is None:
            raise KeyError("Incident not found")
        return dict(row)

    def incidents(self):
        """
            按创建时间列出事件；G0 尚未增加分页和复杂过滤。

            Args:
                无显式参数。

            Returns:
                list[dict]: 按创建时间排列的事件记录；无事件时为空列表。
        """
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM incidents ORDER BY created_at")]

    def start_run(self, incident_id: str, model: str):
        """
            同一事务创建 RUNNING 记录并将事件改为 INVESTIGATING，返回运行 ID。

            同一事件的并发请求由唯一索引裁决；冲突抛 ConflictError。
            这里仅做运行登记，不负责领取异步任务或恢复被强制终止的调查。

            Args:
                incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。
                model (str): 运行记录中保存的模型标识，不在此方法内调用模型。

            Returns:
                str: 新建的 run- 前缀运行 ID。
        """
        # 去重依赖数据库部分唯一索引，避免“先检查再插入”的并发竞态。
        self.incident(incident_id)
        run_id = f"run-{uuid4().hex}"
        with self.connection() as db:
            try:
                db.execute(
                    "INSERT INTO runs (id, incident_id, model, state, started_at) "
                    "VALUES (?, ?, ?, 'RUNNING', ?)",
                    (run_id, incident_id, model, utc_now()),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("Incident already has a running investigation") from exc
            # 和运行插入同事务提交；插入冲突时回滚，事件不会被误标为调查中。
            db.execute("UPDATE incidents SET state='INVESTIGATING' WHERE id=?", (incident_id,))
        return run_id

    def finish_run(self, run_id: str, report=None, error: str | None = None):
        """
            提交调查结果，同时更新运行记录和所属事件的业务状态。

            有 error 时运行为 FAILED，否则为 COMPLETED；报告为 proposed 时事件
            标为 PROPOSED，其余情况为 NEEDS_HUMAN。调用者负责在进入此方法前校验报告。
            只有 RUNNING 可以转换为终态；并发或迟到的结束请求不能覆盖旧结果及新调查状态。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                report: 调用方已校验的最终报告字典；失败时应为 None。
                error (str | None): 失败原因；成功时应为 None，提供时必须为非空白字符串。

            Returns:
                None: 原子更新运行终态与关联事件状态，不宣告故障恢复。
        """
        # 调查完成只代表得到报告；G0 无独立恢复验收，事件永远不在此转为已解决。
        run = self.run(run_id)
        # 按 None 判断“恰好一个结果”，避免空字典或空文本的真假值掩盖调用方的互斥参数错误。
        if (report is None) == (error is None):
            raise ValueError("Provide exactly one of report or error")
        if error is not None and not error.strip():
            raise ValueError("Investigation error must not be empty")
        state = "FAILED" if error else "COMPLETED"
        incident_state = "PROPOSED" if report and report["status"] == "proposed" else "NEEDS_HUMAN"
        with self.connection() as db:
            updated = db.execute(
                "UPDATE runs SET state=?, finished_at=?, report=?, error=? "
                "WHERE id=? AND state='RUNNING'",
                (state, utc_now(), json.dumps(report) if report else None, error, run_id),
            )
            # 终态更新必须实际抢到 RUNNING 状态；否则不能继续改事件状态，以免旧请求覆盖新调查。
            if updated.rowcount != 1:
                raise ConflictError("Investigation already finished")
            db.execute(
                "UPDATE incidents SET state=? WHERE id=?", (incident_state, run["incident_id"])
            )

    def run(self, run_id: str):
        """
            返回运行记录并将 report JSON 还原为字典；尚无报告时保持 None。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                dict: 运行详情，report 字段从 JSON 解码为字典或 None。
        """
        with self.connection() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError("Investigation run not found")
        result = dict(row)
        result["report"] = json.loads(result["report"]) if result["report"] else None
        return result

    def save_evidence(self, run_id: str, tool: str, raw: dict):
        """
            保存工具原始结果，返回新证据 ID；不会覆盖已有证据文件。

            run_id 决定证据归属，tool 记录来源，raw 应为 JSON 可序列化字典。
            先独占写文件再登记哈希，落库失败可能留下无索引文件，但不会发布缺失正文
            的新索引；该顺序不等于文件系统与 SQLite 之间具有原子提交能力。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                tool (str): 产生观测的工具名，用于记录证据来源。
                raw (dict): 待独占保存的原始观测字典，必须能编码为有限数值 JSON。

            Returns:
                str: 新建的 ev- 前缀证据 ID。
        """
        run = self.run(run_id)
        evidence_id = f"ev-{uuid4().hex}"
        # 拒绝非有限值，避免把 NaN/Infinity 当成正常观测或写出难以跨实现重放的证据。
        body = json.dumps(raw, ensure_ascii=False, allow_nan=False).encode()
        path = self.root / "evidence" / f"{evidence_id}.json"
        # 独占创建保证已有证据不会因文件名碰撞被覆盖；先写正文再发布索引，避免索引指向缺失内容。
        with path.open("xb") as output:
            output.write(body)
        with self.connection() as db:
            db.execute(
                "INSERT INTO evidence VALUES (?, ?, ?, ?, ?, ?)",
                (
                    evidence_id,
                    run["incident_id"],
                    run_id,
                    tool,
                    utc_now(),
                    hashlib.sha256(body).hexdigest(),
                ),
            )
        return evidence_id

    def read_evidence(self, run_id: str, evidence_id: str):
        """
            返回索引元数据和 raw 正文；归属不符抛 KeyError，哈希不符抛 ValueError。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                evidence_id (str): 平台生成的证据 ID；需要同时校验所属调查运行。

            Returns:
                dict: 证据索引元数据与校验通过的 raw 原文。
        """
        # 同时匹配 run_id，防止模型使用其他调查的证据；读取时再验证内容未被改动。
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM evidence WHERE id=? AND run_id=?", (evidence_id, run_id)
            ).fetchone()
        if row is None:
            raise KeyError("Evidence not found in this investigation")
        # 用已匹配的数据库 ID 构造文件名，不直接把调用者提供的字符串作为文件路径。
        body = (self.root / "evidence" / f"{row['id']}.json").read_bytes()
        if hashlib.sha256(body).hexdigest() != row["sha256"]:
            raise ValueError("Evidence integrity check failed")
        return {**dict(row), "raw": json.loads(body)}

    def evidence_ids(self, run_id: str):
        """
            返回本次运行可引用的证据 ID 集合，用于最终报告的确定性归属校验。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                set[str]: 该运行的证据 ID 集合；没有匹配记录时为空集合。
        """
        with self.connection() as db:
            return {
                row[0] for row in db.execute("SELECT id FROM evidence WHERE run_id=?", (run_id,))
            }

    def trace(self, run_id: str, event: dict | None = None):
        """
            传 event 时追加带 UTC 时间的轨迹，否则按写入顺序返回全部轨迹。

            先验证运行存在，避免创建无归属文件；锁只协调同一 Store 实例的线程。
            这不是跨进程日志锁，也不承担任务队列或图 checkpoint 的作用。

            Args:
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。
                event (dict | None): 待追加的事件字典；为 None 时只读已有轨迹，不追加记录。

            Returns:
                list[dict] | None: 读取时返回轨迹列表，写入时返回 None；无文件返回空列表。
        """
        self.run(run_id)
        path = self.root / "traces" / f"{run_id}.jsonl"
        with self._trace_lock:
            # None 专门表示读取；空字典仍是一条需要记录时间的事件，不能用真假值区分。
            if event is not None:
                with path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps({"at": utc_now(), **event}, ensure_ascii=False) + "\n")
                return None
            return (
                [json.loads(line) for line in path.read_text().splitlines()]
                if path.exists()
                else []
            )
