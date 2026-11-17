"""管理一个本地真实进程会话：启动、观测、故障注入及退出清理。"""

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from robotops.models import utc_now
from robotops.observations import ObservationUnavailable
from robotops.storage import ConflictError

from .worker import read_json, write_json


def process_info(pid: int):
    """读取受管 PID 的 /proc 信息，返回存活状态、启动标识、内存和累计 CPU 时间。

    调用者仅传入本会话拥有的 PID。start_ticks 用来识别同一 PID 被其他进程复用；
    Z/X 状态视为已退出，CPU 时间是累计秒数而不是 CPU 使用率百分比。
    进程在读取前消失时返回 alive=False，其余文件访问错误交给上层处理。
    """
    try:
        # comm 字段在括号中，可能包含空格；先从最后一个右括号切分再按固定字段取值。
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        rss = int(stat[21]) * os.sysconf("SC_PAGE_SIZE")
        return {
            "pid": pid,
            "alive": stat[0] not in {"Z", "X"},
            "state": stat[0],
            "start_ticks": stat[19],
            "rss_bytes": rss,
            "cpu_time_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        }
    except (FileNotFoundError, ProcessLookupError):
        return {"pid": pid, "alive": False}


class PipelineRuntime:
    """单个会话的进程所有者，所有公开操作通过同一把可重入锁协调。

    每次 start 创建独立目录和实例 ID，避免旧遥测混入新会话。
    使用 RLock 是因为 start 的失败清理、状态返回还会调用本对象的 stop/snapshot。
    这里只跟踪当前 Python 进程创建的子进程，不接管重启前遗留的运行实例。
    """

    def __init__(self, root: Path):
        self.root = root / "demo"
        self.runtime: Path | None = None
        self.processes: dict[str, subprocess.Popen] = {}
        self.expected_starts = {}
        self._lock = threading.RLock()

    def start(self):
        """启动处理端、取得实际绑定端口，再启动生产端，最后返回运行快照。

        只允许一个活跃会话；任一组件启动失败都先清理已创建子进程再向上传递异常。
        处理端端口由操作系统分配，避免先选端口再绑定导致的占用竞态。
        """
        with self._lock:
            if self.runtime is not None:
                raise ConflictError("Demo session already exists; stop it before starting another")
            self.runtime = self.root / uuid4().hex
            try:
                # 文件初始化同样可能失败，必须与子进程启动共用清理路径。
                self.runtime.mkdir(parents=True)
                write_json(self.runtime / "private-controls.json", {})
                self._set_config("scan")
                self._spawn("processor")
                port = read_json(self.runtime / "processor.json")["bound_port"]
                self._spawn("producer", port)
            except Exception:
                self.stop()
                raise
            return self.snapshot()

    def _spawn(self, component: str, port: int = 0):
        """用当前解释器启动固定 worker 模块，等待匹配 instance_id 的首份遥测。

        component/port 来自本管理器，不直接接受 Agent 参数；不经 shell 拼接执行。
        stderr 独立落盘便于调查启动失败，超时和提前退出均抛 ObservationUnavailable。
        """
        instance = uuid4().hex
        errors = self.runtime / f"{component}.stderr.log"
        with errors.open("w") as error_output:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "robotops_examples.pipeline.worker",
                    component,
                    "--runtime",
                    str(self.runtime),
                    "--instance",
                    instance,
                    "--port",
                    str(port),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=error_output,
            )
        self.processes[component] = process
        self.expected_starts[component] = process_info(process.pid).get("start_ticks")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise ObservationUnavailable(f"{component} exited during startup; inspect {errors}")
            sample = read_json(self.runtime / f"{component}.json")
            if sample.get("instance_id") == instance:
                return
            time.sleep(0.02)
        raise ObservationUnavailable(f"{component} startup timed out")

    def _set_config(self, expected_channel: str):
        """只在期望输入通道变化时递增配置版本，并记录变更前后值供诊断检索。"""
        old = read_json(self.runtime / "config.json")
        if old.get("expected_channel") == expected_channel:
            return
        current = {"version": old.get("version", 0) + 1, "expected_channel": expected_channel}
        write_json(self.runtime / "config.json", current)
        with (self.runtime / "config_changes.jsonl").open("a") as output:
            output.write(json.dumps({"at": utc_now(), "before": old, "after": current}) + "\n")

    def inject(self, mode: str):
        """将开发者选择的场景施加到真实 worker，返回场景名和所属会话。

        停发/停滞通过私有控制文件实现；配置错误修改实际期望通道；退出终止处理端。
        normal 解除控制并恢复正确配置，但不会重新启动已退出的子进程。
        场景真值和私有控制文件不通过 Agent 工具暴露，Agent 只能查询实际运行现象。
        """
        with self._lock:
            if self.runtime is None:
                raise ObservationUnavailable("Demo is not running")
            if mode not in {
                "normal",
                "input_stopped",
                "processing_stalled",
                "config_mismatch",
                "processor_exit",
            }:
                raise ValueError("Unsupported demo fault")
            write_json(
                self.runtime / "private-controls.json",
                {
                    "pause_source": mode == "input_stopped",
                    "stall_processor": mode == "processing_stalled",
                },
            )
            self._set_config("wrong-channel" if mode == "config_mismatch" else "scan")
            if mode == "processor_exit":
                self._terminate(self.processes["processor"])
            return {"applied": mode, "runtime_id": self.runtime.name}

    def snapshot(self):
        """组合操作系统进程事实与 worker 遥测，不把旧遥测当作当前存活证明。

        先回收退出子进程，再比较启动标识识别 PID 复用；遥测可能保留退出前的内容，
        使用方应结合 process.alive、实例和观测时间解释，不能只看最后一份计数。
        """
        with self._lock:
            if self.runtime is None:
                raise ObservationUnavailable("Demo is not running; observations are unavailable")
            components = {}
            for component, process in self.processes.items():
                process.poll()  # 回收已退出子进程，避免留下僵尸进程。
                observed = process_info(process.pid)
                if observed.get("start_ticks") != self.expected_starts.get(component):
                    observed["alive"] = False
                try:
                    telemetry = read_json(self.runtime / f"{component}.json")
                except json.JSONDecodeError as exc:
                    raise ObservationUnavailable(f"Invalid telemetry for {component}") from exc
                if not isinstance(telemetry, dict):
                    raise ObservationUnavailable(f"Invalid telemetry for {component}")
                components[component] = {
                    "process": observed,
                    "telemetry": telemetry,
                }
            return {
                "app_id": "pipeline",
                "runtime_id": self.runtime.name,
                "observed_at": utc_now(),
                "components": components,
            }

    def public_file(self, name: str):
        """返回白名单 JSONL 的有效记录、会话归属及读取完整性说明。

        最多读取文件末尾 1 MiB；截断时丢弃可能不完整的首行，避免解析半条 JSON。
        追加中的末行暂不解析并标记 incomplete_tail；完整但损坏的行计入 malformed_lines。
        有效行必须是带时区 at 时间的对象，避免单条坏记录导致全部日志不可检索。
        文件尚未产生时返回空记录；空日志或部分读取均不能证明应用健康。
        """
        with self._lock:
            if self.runtime is None:
                raise ObservationUnavailable("Demo is not running")
            if name not in {"producer.jsonl", "processor.jsonl", "config_changes.jsonl"}:
                raise ValueError("File is not a diagnostic source")
            path = self.runtime / name
            result = {
                "runtime_id": self.runtime.name,
                "rows": [],
                "truncated": False,
                "malformed_lines": 0,
                "incomplete_tail": False,
            }
            if not path.exists():
                return result
            # ponytail: 当前只读末尾 1 MiB；日志留存增大后改用有索引的日志存储。
            with path.open("rb") as source:
                source.seek(0, 2)
                size = source.tell()
                result["truncated"] = size > 1024 * 1024
                source.seek(max(0, size - 1024 * 1024))
                if result["truncated"]:
                    source.readline()
                body = source.read()
            lines = body.splitlines()
            if body and not body.endswith(b"\n"):
                result["incomplete_tail"] = True
                lines.pop()
            for line in lines:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line.decode("utf-8"))
                    if not isinstance(row, dict) or not isinstance(row.get("at"), str):
                        raise ValueError("Log record requires a timestamp")
                    if datetime.fromisoformat(row["at"]).utcoffset() is None:
                        raise ValueError("Log timestamp must include a timezone")
                except (ValueError, UnicodeDecodeError):
                    result["malformed_lines"] += 1
                    continue
                result["rows"].append(row)
            return result

    @staticmethod
    def _terminate(process):
        """先请求退出并等待，超时后强制终止再回收；仅作用于已持有的子进程对象。"""
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)

    def stop(self):
        """仅清理本会话拥有的 Popen 子进程，不按进程名搜索和终止其他应用。"""
        with self._lock:
            for process in self.processes.values():
                self._terminate(process)
            self.processes.clear()
            self.expected_starts.clear()
            self.runtime = None
