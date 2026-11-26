"""
    业务持久化：SQLite 保存关联关系，独立文件保存原始证据和逐轮调查轨迹。
"""

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .models import (
    ApplicationContract,
    IncidentCreate,
    ServiceObservation,
    ServiceRegistration,
    utc_now,
)


class ConflictError(ValueError):
    """
        应用重复注册或同事件已有活跃调查等业务冲突，由 API 映射为 409。
    """

    pass


ROS_TOPIC_EVIDENCE = "ros1_topic_observation"


def _ros_evidence_body(fact):
    return {
        "status": "ok",
        "observed_at": datetime.fromtimestamp(fact["observed_at_ms"] / 1000, timezone.utc).isoformat(),
        "data": fact,
    }


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
                BEGIN IMMEDIATE;
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
                CREATE TABLE IF NOT EXISTS context_inputs (
                    sha256 TEXT PRIMARY KEY, body TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS context_messages (
                    sha256 TEXT PRIMARY KEY, body TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS managed_services (
                    app_id TEXT PRIMARY KEY REFERENCES applications,
                    scope TEXT NOT NULL, unit TEXT NOT NULL, name TEXT NOT NULL,
                    config_version INTEGER NOT NULL DEFAULT 1,
                    observation TEXT NOT NULL, last_known TEXT,
                    armed INTEGER NOT NULL, last_running_instance TEXT,
                    UNIQUE(scope, unit)
                );
                CREATE TABLE IF NOT EXISTS service_exits (
                    incident_id TEXT PRIMARY KEY REFERENCES incidents,
                    app_id TEXT NOT NULL REFERENCES managed_services,
                    config_version INTEGER NOT NULL,
                    occurrences INTEGER NOT NULL,
                    first_observation TEXT NOT NULL, latest_observation TEXT NOT NULL,
                    first_instance_id TEXT, latest_instance_id TEXT
                );
                CREATE INDEX IF NOT EXISTS service_exit_app ON service_exits(app_id);
                CREATE TABLE IF NOT EXISTS recovery_actions (
                    id TEXT PRIMARY KEY,
                    incident_id TEXT NOT NULL UNIQUE REFERENCES service_exits(incident_id),
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    unit TEXT NOT NULL, scope TEXT NOT NULL,
                    config_version INTEGER NOT NULL, failed_instance_id TEXT,
                    basis TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS executions (
                    id TEXT PRIMARY KEY, app_id TEXT NOT NULL REFERENCES applications,
                    started_at TEXT NOT NULL, finished_at TEXT, returncode INTEGER
                );
                CREATE TABLE IF NOT EXISTS execution_exits (
                    incident_id TEXT PRIMARY KEY REFERENCES incidents,
                    execution_id TEXT NOT NULL REFERENCES executions,
                    process_name TEXT NOT NULL, exit_code INTEGER NOT NULL, log_end INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS launch_restarts (
                    incident_id TEXT PRIMARY KEY REFERENCES execution_exits,
                    execution_id TEXT NOT NULL UNIQUE REFERENCES executions,
                    state TEXT NOT NULL, checked_at TEXT, topic_acceptance TEXT
                );
                CREATE TABLE IF NOT EXISTS ros_fact_tasks (
                    source_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                    incident_id TEXT NOT NULL UNIQUE REFERENCES incidents,
                    fact TEXT NOT NULL, run_id TEXT UNIQUE REFERENCES runs,
                    PRIMARY KEY (source_id, sequence)
                );
            """)
            if not any(col["name"] == "topic_acceptance" for col in db.execute("PRAGMA table_info(launch_restarts)")):
                db.execute("ALTER TABLE launch_restarts ADD COLUMN topic_acceptance TEXT")

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
        with self.connection() as db:
            incident_id = self._insert_incident(db, request, utc_now())
        return self.incident(incident_id)

    @staticmethod
    def _insert_incident(db: sqlite3.Connection, request: IncidentCreate, timestamp: str) -> str:
        """
            在调用方的事务内写入一条事件，供手工接入和检测器共用。

            Args:
                db: 已开启的数据库连接。
                request: 关联应用、事件症状与来源。
                timestamp: 手工提交或第一次检测到异常的 UTC 时间。

            Returns:
                str: 新建事件的编号。
        """
        incident_id = f"inc-{uuid4().hex}"
        db.execute(
            "INSERT INTO incidents VALUES (?, ?, ?, ?, 'OPEN', ?)",
            (incident_id, request.app_id, request.symptom, request.source, timestamp),
        )
        return incident_id

    def ingest_ros_fact(self, app_id: str, fact: dict) -> str | None:
        """Persist a new observation and its task, returning the incident ID."""
        identity = (fact["source_id"], fact["sequence"])
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM ros_fact_tasks WHERE source_id=? AND sequence=?", identity,
            ).fetchone():
                return None
            request = IncidentCreate(
                app_id=app_id, source="detector",
                symptom=f"ROS 1 Topic {fact['topic']} had no messages for {fact['silent_ms']} ms",
            )
            incident_id = self._insert_incident(db, request, utc_now())
            db.execute(
                "INSERT INTO ros_fact_tasks (source_id, sequence, incident_id, fact) "
                "VALUES (?, ?, ?, ?)",
                (*identity, incident_id, json.dumps(fact, ensure_ascii=False)),
            )
        return incident_id

    def interrupt_ros_fact_runs(self, app_id: str) -> None:
        """Reconcile interrupted work after taking the app's exclusive worker lock."""
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute(
                "SELECT r.id, r.incident_id FROM runs r JOIN ros_fact_tasks t ON t.run_id=r.id "
                "JOIN incidents i ON i.id=r.incident_id WHERE i.app_id=? AND r.state='RUNNING'",
                (app_id,),
            ).fetchall()
            for row in active:
                db.execute(
                    "UPDATE runs SET state='FAILED', finished_at=?, error=? WHERE id=?",
                    (utc_now(), "Automatic ROS investigation was interrupted", row["id"]),
                )
                db.execute(
                    "UPDATE incidents SET state='NEEDS_HUMAN' WHERE id=?", (row["incident_id"],),
                )

    def queued_ros_fact_incidents(self, app_id: str, *, limit: int = -1) -> list[str]:
        """Find imported observations whose automatic investigations have not started."""
        with self.connection() as db:
            return [row[0] for row in db.execute(
                "SELECT t.incident_id FROM ros_fact_tasks t JOIN incidents i ON i.id=t.incident_id "
                "WHERE i.app_id=? AND i.state='OPEN' AND t.run_id IS NULL ORDER BY t.rowid LIMIT ?",
                (app_id, limit),
            )]

    def register_service(self, request: ServiceRegistration, observation: ServiceObservation) -> dict:
        """
            为已有服务生成最小应用契约，并在同一事务保存服务登记和首份观测。

            Args:
                request: 调用者填写的服务和可选显示名。
                observation: 登记前读取的状态，包含 systemd 返回的规范名称。

            Returns:
                dict: 已保存的登记、当前状态和配置版本。
        """
        if not observation.unit:
            raise ValueError("A readable service observation is required for registration")
        name = (request.name or "").strip() or observation.unit
        app_id = f"svc-{uuid4().hex}"
        contract = ApplicationContract(app_id=app_id, components=["service"], description=name)
        sample = observation.model_dump_json()
        running = observation.state == "running"
        with self.connection() as db:
            db.execute("INSERT INTO applications VALUES (?, ?)", (app_id, contract.model_dump_json()))
            try:
                db.execute(
                    "INSERT INTO managed_services "
                    "(app_id, scope, unit, name, observation, last_known, armed, last_running_instance) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        app_id, request.scope, observation.unit, name, sample,
                        sample if observation.state != "unknown" else None,
                        int(running), observation.instance_id if running else None,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                # 规范名称唯一约束使别名与原名称的重复登记一并回滚。
                raise ConflictError("Service already registered in this scope") from exc
        return self.services(app_id)[0]

    def services(self, app_id: str | None = None) -> list[dict]:
        """
            读取已登记服务及最新状态。查询失败后仍保留上一次可解读的观测。

            Args:
                app_id: 可选应用编号，为 None 时列出全部服务。

            Returns:
                list[dict]: 服务配置、当前与最后有效观测。
        """
        fields = (
            "SELECT app_id, scope, unit, name, config_version, observation, last_known "
            "FROM managed_services"
        )
        with self.connection() as db:
            rows = db.execute(
                fields + " ORDER BY scope, unit" if app_id is None else fields + " WHERE app_id=?",
                () if app_id is None else (app_id,),
            ).fetchall()
        if app_id is not None and not rows:
            raise KeyError("Managed service not found")
        result = []
        for row in rows:
            service = dict(row)
            for field in ("observation", "last_known"):
                service[field] = json.loads(row[field]) if row[field] else None
            result.append(service)
        return result

    def record_service_observation(
        self, app_id: str, observation: ServiceObservation,
    ) -> dict:
        """
            持久化读数，首次确认运行后的失败创建事件，持续异常续记原事件。

            Args:
                app_id: 服务登记生成的应用编号。
                observation: 本次只读探测的结果，包含探测开始时间。

            Returns:
                dict: 当前状态、关联事件编号与本次是否创建事件。
        """
        with self.connection() as db:
            # 读取基线与写事件用同一写事务。并发监测进程不能同时创建两个活跃事件。
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM managed_services WHERE app_id=?", (app_id,)).fetchone()
            if row is None:
                raise KeyError("Managed service not found")
            previous = ServiceObservation.model_validate_json(row["observation"])
            if observation.observed_at <= previous.observed_at:
                return {"state": previous.state, "incident_id": None, "created": False}
            if observation.unit != row["unit"] and (observation.unit is not None or observation.state != "unknown"):
                # 规范名称变化可能意味着别名被重新绑定。旧登记不得替新目标作故障判断。
                observation = ServiceObservation(
                    observed_at=observation.observed_at, state="unknown", detail="服务规范名称发生变化"
                )
            state = observation.state
            sample = observation.model_dump_json()
            last_known = row["last_known"] if state == "unknown" else sample
            armed = (state == "running") if state in {"running", "stopped"} else bool(row["armed"])
            instance = observation.instance_id if state == "running" else row["last_running_instance"]
            db.execute(
                "UPDATE managed_services SET observation=?, last_known=?, armed=?, last_running_instance=? "
                "WHERE app_id=?",
                (sample, last_known, int(armed), instance, app_id),
            )
            if state != "failed" or not armed:
                # 首次登记时已经停止、干净停止和观测不可用都不构成新的意外退出。
                return {"state": state, "incident_id": None, "created": False}

            active = db.execute(
                "SELECT e.incident_id FROM service_exits e "
                "JOIN incidents i ON i.id=e.incident_id "
                "WHERE e.app_id=? AND i.state!='RESOLVED'",
                (app_id,),
            ).fetchone()
            failed_instance = observation.instance_id or instance
            created = active is None
            if active is None:
                incident_id = self._insert_incident(db, IncidentCreate(
                    app_id=app_id,
                    symptom=f"服务 {row['unit']} 意外退出（{observation.result}）",
                    source="detector",
                ), observation.observed_at.isoformat())
                db.execute(
                    "INSERT INTO service_exits VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        incident_id, app_id, row["config_version"], sample, sample,
                        failed_instance, failed_instance,
                    ),
                )
            else:
                incident_id = active["incident_id"]
                db.execute(
                    "UPDATE service_exits SET latest_observation=?, latest_instance_id=?, "
                    "occurrences=occurrences+1 WHERE incident_id=?",
                    (sample, failed_instance, incident_id),
                )
            return {"state": state, "incident_id": incident_id, "created": created}

    def start_execution(
        self, app_id: str, execution_id: str, *, restart_of: str | None = None,
        topic_acceptance: dict | None = None,
    ) -> None:
        """
            首次启动时自动登记最小契约，并保存独立运行标识。

            Args:
                app_id: 操作员指定的应用名。
                execution_id: 关联本次终端输出的唯一编号。
                restart_of: 本次执行要恢复的事件编号，普通启动时为空。
                topic_acceptance: 本次重启的可选 Topic 验收目标与阈值。

            Returns:
                None: 登记和运行记录已一起提交。
        """
        contract = ApplicationContract(app_id=app_id, components=["process"])
        with self.connection() as db:
            # 同一应用可由多个终端同时启动；已有契约的组件和版本以显式登记为准。
            db.execute(
                "INSERT INTO applications VALUES (?, ?) ON CONFLICT(app_id) DO NOTHING",
                (app_id, contract.model_dump_json()),
            )
            db.execute(
                "INSERT INTO executions (id, app_id, started_at) VALUES (?, ?, ?)",
                (execution_id, app_id, utc_now()),
            )
            if restart_of is not None:
                db.execute(
                    "INSERT INTO launch_restarts (incident_id, execution_id, state, topic_acceptance) "
                    "VALUES (?, ?, 'STARTING', ?)",
                    (restart_of, execution_id, json.dumps(topic_acceptance) if topic_acceptance else None),
                )

    def finish_launch_restart(self, execution_id: str, state: str) -> None:
        """Record the first startup check result for the replacement launch."""
        with self.connection() as db:
            db.execute(
                "UPDATE launch_restarts SET state=?, checked_at=? "
                "WHERE execution_id=? AND state='STARTING'",
                (state, utc_now(), execution_id),
            )

    def finish_topic_acceptance(self, execution_id: str, result: dict) -> None:
        """Keep one final observation on the existing restart receipt."""
        with self.connection() as db:
            db.execute(
                "UPDATE launch_restarts SET topic_acceptance=? WHERE execution_id=? "
                "AND json_extract(topic_acceptance, '$.state')='PENDING'",
                (json.dumps({**result, "checked_at": utc_now()}, allow_nan=False), execution_id),
            )

    def finish_execution(self, execution_id: str, returncode: int) -> None:
        """
            记录本次启动命令的最终退出状态。

            Args:
                execution_id: 此次前台运行编号。
                returncode: 原始进程返回码，负值表示收到信号。

            Returns:
                None: 退出状态已提交。
        """
        with self.connection() as db:
            db.execute(
                "UPDATE executions SET finished_at=?, returncode=? WHERE id=?",
                (utc_now(), returncode, execution_id),
            )

    def record_execution_exit(
        self, execution_id: str, process_name: str, exit_code: int, log_end: int,
    ) -> str:
        """
            记录一次异常退出及对应的控制台日志位置。

            Args:
                execution_id: 已登记的前台启动编号。
                process_name: 启动命令或 ROS 提供的进程名。
                exit_code: 进程返回码。
                log_end: 事件产生时已写入日志的字节数。

            Returns:
                str: 可用于查询故障和日志的事件编号。
        """
        with self.connection() as db:
            app = db.execute("SELECT app_id FROM executions WHERE id=?", (execution_id,)).fetchone()
            if app is None:
                raise KeyError("Execution not found")
            incident_id = self._insert_incident(
                db, IncidentCreate(
                    app_id=app["app_id"], symptom=f"进程 {process_name[:512]} 意外退出（{exit_code}）",
                    source="detector",
                ), utc_now(),
            )
            # 日志仍可随着其他节点继续运行增长，固定位置保证后续查询读到的是故障当时的窗口。
            db.execute(
                "INSERT INTO execution_exits VALUES (?, ?, ?, ?, ?)",
                (incident_id, execution_id, process_name, exit_code, log_end),
            )
        return incident_id

    def execution_logs(
        self, incident_id: str, keyword: str = "", limit: int = 30, since: datetime | None = None,
    ) -> dict:
        """
            从事件关联的日志读取故障前的控制台片段。

            Args:
                incident_id: 事件编号，由工具网关绑定到当前调查。
                keyword: 可选的字面关键词，忽略大小写。
                limit: 最多返回的匹配行数。
                since: 起始时间，用于筛选事件快照，日志行沿用进程自身时间格式。

            Returns:
                dict: 故障进程、采集范围、日志行与截断标记。
        """
        with self.connection() as db:
            record = db.execute(
                "SELECT i.created_at, e.execution_id, e.process_name, e.log_end FROM incidents i "
                "LEFT JOIN execution_exits e ON e.incident_id=i.id WHERE i.id=?",
                (incident_id,),
            ).fetchone()
        if record is None:
            raise KeyError("Incident not found")
        if record["execution_id"] is None:
            raise KeyError("No console log for this incident")
        end = record["log_end"]
        # 按字节向前读取使大文件查询成本稳定；输出其余部分仍可从本地原文件查看。
        start = max(0, end - 64 * 1024)
        observed_at = record["created_at"]
        within_range = since is None or datetime.fromisoformat(observed_at) >= since
        if not within_range:
            lines = []
        else:
            path = self.root / "executions" / f"{record['execution_id']}.log"
            with path.open("rb") as log:
                log.seek(max(0, start - 1))
                fragment = log.read(end - max(0, start - 1))
            if start:
                # 首字节用于判断是否落在行边界；残缺行可能误导关键词匹配。
                fragment = fragment[1:] if fragment.startswith(b"\n") else fragment.partition(b"\n")[2]
            needle = keyword.casefold()
            lines = [
                line for line in fragment.decode(errors="replace").splitlines()
                if needle in line.casefold()
            ]
        return {
            "source": "captured_console", "runtime_id": record["execution_id"],
            "process_name": record["process_name"], "captured_at": observed_at,
            "lines": lines[-limit:], "truncated": within_range and bool(start or len(lines) > limit),
        }

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
            exit_record = db.execute(
                "SELECT config_version, occurrences, first_observation, latest_observation, "
                "first_instance_id, latest_instance_id FROM service_exits WHERE incident_id=?",
                (incident_id,),
            ).fetchone()
            recovery_action = None
            if exit_record is not None:
                recovery_action = db.execute(
                    "SELECT id, kind, state, unit, scope, config_version, failed_instance_id, "
                    "basis, created_at FROM recovery_actions WHERE incident_id=?",
                    (incident_id,),
                ).fetchone()
            execution_exit = db.execute(
                "SELECT execution_id, process_name, exit_code, log_end "
                "FROM execution_exits WHERE incident_id=?", (incident_id,),
            ).fetchone()
            launch_restart = None
            if execution_exit is not None:
                launch_restart = db.execute(
                    "SELECT execution_id, state, checked_at, topic_acceptance "
                    "FROM launch_restarts WHERE incident_id=?",
                    (incident_id,),
                ).fetchone()
            fact_task = db.execute(
                "SELECT t.fact, t.run_id, r.state FROM ros_fact_tasks t "
                "LEFT JOIN runs r ON r.id=t.run_id WHERE t.incident_id=?", (incident_id,),
            ).fetchone()
        result = dict(row)
        if exit_record is not None:
            extension = dict(exit_record)
            for field in ("first_observation", "latest_observation"):
                extension[field] = json.loads(extension[field])
            result["service_exit"] = extension
        if recovery_action is not None:
            result["recovery_action"] = {
                **dict(recovery_action), "basis": json.loads(recovery_action["basis"]),
            }
        if execution_exit is not None:
            result["execution_exit"] = dict(execution_exit)
        if launch_restart is not None:
            restart = dict(launch_restart)
            acceptance = restart.pop("topic_acceptance")
            if acceptance is not None:
                restart["topic_acceptance"] = json.loads(acceptance)
            result["launch_restart"] = restart
        if fact_task is not None:
            result["ros_fact"] = json.loads(fact_task["fact"])
            result["automatic_run_id"] = fact_task["run_id"]
            result["automatic_task_state"] = fact_task["state"] or "QUEUED"
        return result

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

    def start_run(self, incident_id: str, model: str, *, automatic: bool = False):
        """
            同一事务创建 RUNNING 记录并将事件改为 INVESTIGATING，返回运行 ID。

            同一事件的并发请求由唯一索引裁决，自动任务在同一事务认领。

            Args:
                incident_id (str): 已登记的异常事件 ID，用于绑定应用与调查状态。
                model (str): 运行记录中保存的模型标识，不在此方法内调用模型。
            automatic (bool): 在同一事务认领已存在的自动任务。

            Returns:
                str: 新建的 run- 前缀运行 ID。
        """
        run_id = f"run-{uuid4().hex}"
        with self.connection() as db:
            try:
                # 只为已登记事件插入记录，活跃运行去重由唯一索引裁决。
                inserted = db.execute(
                    "INSERT INTO runs (id, incident_id, model, state, started_at) "
                    "SELECT ?, id, ?, 'RUNNING', ? FROM incidents WHERE id=?",
                    (run_id, model, utc_now(), incident_id),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("Incident already has a running investigation") from exc
            if inserted.rowcount == 0:
                raise KeyError("Incident not found")
            if automatic:
                claimed = db.execute(
                    "UPDATE ros_fact_tasks SET run_id=? WHERE incident_id=? AND run_id IS NULL",
                    (run_id, incident_id),
                )
                if claimed.rowcount != 1:
                    raise ConflictError("Automatic investigation task already claimed or missing")
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
        incident_id = self.run_incident_id(run_id)
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
                "UPDATE incidents SET state=? WHERE id=?", (incident_state, incident_id)
            )

    def run_incident_id(self, run_id: str) -> str:
        """确认运行存在并返回其事件归属，供独立的审计记录使用。"""
        with self.connection() as db:
            row = db.execute("SELECT incident_id FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError("Investigation run not found")
        return row["incident_id"]

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
        return self._save_evidence(run_id, tool, raw, write_file=True)

    def save_ros_fact_evidence(self, run_id: str, fact: dict) -> str:
        """Expose the observation already stored by ROS ingestion as run-scoped evidence."""
        return self._save_evidence(run_id, ROS_TOPIC_EVIDENCE, _ros_evidence_body(fact), write_file=False)

    def _save_evidence(self, run_id: str, tool: str, raw: dict, *, write_file: bool) -> str:
        incident_id = self.run_incident_id(run_id)
        evidence_id = f"ev-{uuid4().hex}"
        # 拒绝非有限值，避免把 NaN/Infinity 当成正常观测或写出难以跨实现重放的证据。
        body = json.dumps(raw, ensure_ascii=False, allow_nan=False).encode()
        if write_file:
            path = self.root / "evidence" / f"{evidence_id}.json"
            # 先保存正文，再发布可以回读的证据索引。
            with path.open("xb") as output:
                output.write(body)
        with self.connection() as db:
            db.execute(
                "INSERT INTO evidence VALUES (?, ?, ?, ?, ?, ?)",
                (
                    evidence_id,
                    incident_id,
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
        path = self.root / "evidence" / f"{row['id']}.json"
        try:
            body = path.read_bytes()
        except FileNotFoundError:
            if row["tool"] != ROS_TOPIC_EVIDENCE:
                raise
            with self.connection() as db:
                source = db.execute(
                    "SELECT fact FROM ros_fact_tasks WHERE incident_id=?", (row["incident_id"],),
                ).fetchone()
            if source is None:
                raise ValueError("ROS fact evidence is unavailable") from None
            raw = _ros_evidence_body(json.loads(source["fact"]))
            body = json.dumps(raw, ensure_ascii=False, allow_nan=False).encode()
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
        self.run_incident_id(run_id)
        path = self.root / "traces" / f"{run_id}.jsonl"
        with self._trace_lock:
            # None 专门表示读取；空字典仍是一条需要记录时间的事件，不能用真假值区分。
            if event is not None:
                with path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps({"at": utc_now(), **event}, ensure_ascii=False) + "\n")
                return None
            events = (
                [json.loads(line) for line in path.read_text().splitlines()]
                if path.exists()
                else []
            )
        model_calls = {}
        for item in events:
            if item.get("type") == "model" and item.get("message"):
                model_calls[item.get("context_sequence")] = {
                    call["id"]: call["function"]["arguments"]
                    for call in item["message"].get("tool_calls") or ()
                }
            if (
                item.get("type") == "tool" and "context_sequence" in item and "arguments" not in item
            ):
                # 参数已经随同轮模型回复记录，查询时保留原有工具事件字段。
                raw = model_calls.get(item["context_sequence"], {}).get(item.get("tool_call_id"))
                try:
                    arguments = json.loads(raw)
                except (TypeError, ValueError):
                    arguments = None
                item["arguments"] = arguments if isinstance(arguments, dict) else None
            if item.get("type") == "prompt" and "context_sequence" in item and "messages" not in item:
                # 兼容轨迹查询的字段，正文只保存到同轮的上下文快照。
                from .agent.context import ContextSnapshots

                try:
                    snapshot = ContextSnapshots(self, run_id).get(item["context_sequence"])
                except (KeyError, ValueError):
                    continue
                item.update(messages=snapshot["messages"], tools=snapshot["tools"])
        return events
