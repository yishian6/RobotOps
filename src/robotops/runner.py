"""
    将前台命令的终端输出和故障事实接进应用、事件和证据链路。
"""

import json
import os
import selectors
import shlex
import shutil
import signal
import socket
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from time import monotonic
from uuid import uuid4

from .storage import Store

LAUNCH_STABILITY_SECONDS = 5


def _roslaunch_command(argv: list[str], event_fd: int) -> list[str]:
    """
        使用 ROS 自己的 Python 启动带退出监听器的 roslaunch 适配器。

        Args:
            argv: 用户原来的 roslaunch 命令和参数。
            event_fd: 只传给本次适配器的事件套接字。

        Returns:
            list[str]: 可以直接执行的 argv，保留原有包名、launch 路径和参数。
    """
    executable = shutil.which(argv[0])
    if executable is None:
        raise FileNotFoundError(f"{argv[0]}: please source the ROS 1 environment")
    # Noetic 的 Python 与 RobotOps 虚拟环境可以不同。沿用原 roslaunch 入口的解释器。
    with Path(executable).open("rb") as script:
        header = script.readline(512).decode(errors="replace").strip()
    interpreter = shlex.split(header[2:]) if header.startswith("#!") else []
    if not interpreter or not Path(interpreter[-1]).name.startswith("python"):
        raise OSError(f"{executable}: expected a ROS 1 Python roslaunch executable")
    adapter = Path(__file__).with_name("roslaunch_adapter.py")
    return [*interpreter, "-u", str(adapter), str(event_fd), *argv[1:]]


def _drain_output(stream, selector: selectors.BaseSelector, log) -> None:
    """
        将已就绪的输出同时送往本地日志与当前终端。

        Args:
            stream: 子进程合并后的标准输出和标准错误流。
            selector: 当前监听器，遇到 EOF 时移除该流。
            log: 本次执行的原始日志。

        Returns:
            None: 已读取的内容按原字节写入两处。
    """
    # 限制一次读取的批次大小，让高频业务输出也能让出处理退出事件和信号的机会。
    for _ in range(32):
        try:
            data = os.read(stream.fileno(), 16384)
        except BlockingIOError:
            break
        if not data:
            selector.unregister(stream)
            break
        log.write(data)
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()


def run_application(
    store: Store, app_id: str, argv: list[str], *, restart_on_exit: bool = False,
) -> int:
    """
        一次命令完成登记、前台运行、输出保存和异常退出入库。

        Args:
            store: 运行与 API 共用的本地存储。
            app_id: 操作员指定的应用标识。
            argv: 要执行的程序及参数，直接传给子进程。
                restart_on_exit: 允许同一 ROS 1 launch 在异常退出后重启一次。

        Returns:
            int: 启动命令的 shell 退出状态，信号退出使用 128 加信号编号。
    """
    if restart_on_exit and (Path(argv[0]).name != "roslaunch" or "-" in argv[1:]):
        raise ValueError("--restart-on-exit requires a roslaunch command with a launch file")
    status, failure = _run_once(store, app_id, argv)
    if restart_on_exit and failure is not None:
        incident_id, _ = failure
        print(f"[robotops] restarting {app_id} once for {incident_id}", file=sys.stderr, flush=True)
        status, failure = _run_once(store, app_id, argv, restart_of=incident_id)
        if not status and failure is not None:
            status = failure[1]
    return 128 - status if status < 0 else status


def _run_once(
    store: Store, app_id: str, argv: list[str], *, restart_of: str | None = None,
) -> tuple[int, tuple[str, int] | None]:
    execution_id = f"exec-{uuid4().hex}"
    log_dir = store.root / "executions"
    log_dir.mkdir(mode=0o700, exist_ok=True)
    log_path = log_dir / f"{execution_id}.log"
    store.start_execution(app_id, execution_id, restart_of=restart_of)
    process_name = Path(argv[0]).name
    stop_signal: int | None = None
    process = None
    required_failure = None
    verify_at = None
    verified = False

    def stop(signum, _frame):
        """
            将操作员的终止信号转给本次启动的进程组，并继续处理已输出的日志。

            Args:
                signum: 收到的 SIGINT 或 SIGTERM。
                _frame: 信号发生时的 Python 栈帧。

            Returns:
                None: 停止请求由子进程处理。
        """
        nonlocal stop_signal
        forwarded = signal.SIGTERM if stop_signal is not None and signum == signal.SIGINT else signum
        stop_signal = signum
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, forwarded)
            except ProcessLookupError:
                # 进程可能恰好在轮询与发信号之间退出，清理由主循环统一完成。
                pass

    with ExitStack() as resources:
        log = resources.enter_context(log_path.open("xb", buffering=0))

        def record_exit(name, code):
            """
                保存事件与当前日志位置，并在终端给出可查询的事件编号。

                Args:
                    name: 退出的启动命令或 ROS 子进程名。
                    code: 原始进程退出码。

                Returns:
                    str: 本次故障的事件编号。
            """
            incident_id = store.record_execution_exit(execution_id, name, code, log.tell())
            print(f"[robotops] {name} exited ({code}), incident {incident_id}", file=sys.stderr, flush=True)
            return incident_id

        selector = resources.enter_context(selectors.DefaultSelector())
        receiver = sender = None
        try:
            if process_name == "roslaunch":
                receiver, sender = socket.socketpair(type=socket.SOCK_DGRAM)
                resources.enter_context(receiver)
                resources.enter_context(sender)
                receiver.setblocking(False)
                selector.register(receiver, selectors.EVENT_READ)
                argv = _roslaunch_command(argv, sender.fileno())
            # 前台终端的信号由 RobotOps 转发一次。独立进程组也便于清理普通命令的子进程。
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous = signal.signal(signum, stop)
                resources.callback(signal.signal, signum, previous)
            env = os.environ.copy()
            # ROS 工具会把运行环境写进原生日志，平台的模型凭据属于 API 进程。
            env.pop("DEEPSEEK_API_KEY", None)
            # Python 在管道模式默认缓存 stdout，使用行式日志的普通 Python 应用也应当及时可见。
            env.setdefault("PYTHONUNBUFFERED", "1")
            process = subprocess.Popen(
                argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                start_new_session=True, pass_fds=(sender.fileno(),) if sender else (),
            )
        except OSError as exc:
            status = 127 if isinstance(exc, FileNotFoundError) else 126
            message = f"[robotops] {process_name}: {exc}"
            log.write((message + "\n").encode())
            print(message, file=sys.stderr, flush=True)
            record_exit(process_name, status)
            store.finish_execution(execution_id, status)
            if restart_of is not None:
                store.finish_launch_restart(execution_id, "FAILED")
            return status, None
        if sender is not None:
            sender.close()
        if stop_signal is not None:
            pending_signal, stop_signal = stop_signal, None
            stop(pending_signal, None)
        try:
            stream = resources.enter_context(process.stdout)
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
            while True:
                returncode = process.poll()
                ready = selector.select(timeout=0.1 if returncode is None else 0)
                if not ready and returncode is not None:
                    break
                readable = {key.fileobj for key, _ in ready}
                # 优先保存终端数据，ROS 退出回调单独到达时也尽量先读到崩溃前已刷新的错误行。
                if stream in readable:
                    _drain_output(stream, selector, log)
                if receiver in readable:
                    for _ in range(32):
                        try:
                            event = json.loads(receiver.recv(8192))
                        except BlockingIOError:
                            break
                        if event["type"] == "started":
                            if restart_of is not None:
                                verify_at = monotonic() + LAUNCH_STABILITY_SECONDS
                        else:
                            # Noetic 回调排除由 launcher 正常关停时停止的节点。
                            incident_id = record_exit(event["name"], event["code"])
                            if event["required"]:
                                required_failure = (incident_id, event["code"])
                if (
                    verify_at is not None and not verified and stop_signal is None
                    and required_failure is None and monotonic() >= verify_at and process.poll() is None
                ):
                    store.finish_launch_restart(execution_id, "STABLE")
                    verified = True
                if returncode is not None and receiver is None:
                    # 普通程序可能把输出描述符留给后台后代，不等它们关闭才交还终端。
                    break
        finally:
            if process.poll() is None:
                stop(signal.SIGTERM, None)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    # 输出写入或数据存储异常时也要回收本次启动，先给 ROS 正常关停的时间。
                    process.kill()
            returncode = process.wait()
            store.finish_execution(execution_id, returncode)
            if restart_of is not None and not verified:
                store.finish_launch_restart(
                    execution_id, "INTERRUPTED" if stop_signal is not None else "FAILED",
                )

        if stop_signal is not None:
            return returncode, None
        if returncode:
            launch_failure = (record_exit(process_name, returncode), returncode)
            return returncode, required_failure or launch_failure
        if required_failure is not None:
            return returncode, required_failure
        return (1 if restart_of is not None and not verified else 0), None
