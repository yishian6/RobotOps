"""
    核心命令行入口，运行应用、启动 API、检查 Prompt 或轮询已登记服务。
"""

import argparse
import json
import sqlite3
from pathlib import Path
from time import sleep

from .agent.prompt_loader import load_investigation_prompt
from .config import Config


def main():
    """
        前台运行一个应用，或执行核心服务、Prompt 检查和监测。

        所有分支只依赖核心模块。可运行示例有自己的模块入口，核心不负责选择示例工厂、
        导入示例代码或分派其烟测命令，保持“示例依赖核心”的单向关系。

        Args:
            无显式参数。

        Returns:
            int | None: run 返回应用退出状态，其他子命令正常退出时返回 None。
    """
    parser = argparse.ArgumentParser(description="RobotOps local runtime and investigation")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the local API")
    serve.add_argument("--port", type=int, default=8000)
    commands.add_parser(
        "check-prompts", help="Validate configured prompt resources without a model"
    )
    monitor = commands.add_parser("monitor", help="Poll registered systemd services without a model")
    monitor.add_argument("--once", action="store_true", help="Poll once and print the results")
    facts = commands.add_parser(
        "watch-ros-facts", help="Receive local ROS 1 facts and automatically investigate new faults",
    )
    facts.add_argument("app_id")
    facts.add_argument("--socket", type=Path, help="Path to the local Unix datagram socket")
    facts.add_argument("--facts-db", type=Path, help="Read-only startup replay source")
    facts.add_argument("--once", action="store_true", help="Investigate one pending or new fact and exit")
    facts.add_argument("--max-model-calls", type=int, default=6, help="Maximum calls per automatic run")
    run = commands.add_parser(
        "run", usage="robotops run [--restart-on-exit] APP_ID -- COMMAND [ARG ...]",
        help="Register and run an application in the foreground",
    )
    run.add_argument(
        "--restart-on-exit", action="store_true",
        help="Restart an unexpectedly exited ROS 1 launch once",
    )
    run.add_argument("app_id")
    run.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn

        uvicorn.run("robotops.api:create_app", factory=True, host="127.0.0.1", port=args.port)
    elif args.command == "check-prompts":
        config = Config.load()
        prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
        print(json.dumps(prompt.metadata, ensure_ascii=False, indent=2))
    elif args.command == "monitor":
        from .monitor import poll_services
        from .service_manager import SystemdServiceManager
        from .storage import Store

        config = Config.load()
        store, manager = Store(config.data_dir), SystemdServiceManager()
        try:
            while True:
                results = poll_services(store, manager)
                if args.once:
                    print(json.dumps(results, ensure_ascii=False))
                    return
                for result in results:
                    # 持续读数已入库。终端只打印新建事件，避免每个采样周期刷屏。
                    if result["created"]:
                        print(json.dumps(result, ensure_ascii=False), flush=True)
                sleep(config.monitor_interval_seconds)
        except KeyboardInterrupt:
            return
    elif args.command == "watch-ros-facts":
        from .ros_facts import watch_facts

        if args.max_model_calls < 1:
            facts.error("expected a positive model-call budget")
        try:
            return watch_facts(
                Config.load(), args.app_id, args.socket, once=args.once,
                max_model_calls=args.max_model_calls, facts_db=args.facts_db,
            )
        except KeyboardInterrupt:
            return
        except (KeyError, OSError, sqlite3.Error, ValueError) as exc:
            facts.error(str(exc))
    elif args.command == "run":
        from .runner import run_application
        from .storage import Store

        if not args.argv:
            run.error("expected an application command")
        try:
            return run_application(
                Store(Config.load().data_dir), args.app_id, args.argv,
                restart_on_exit=args.restart_on_exit,
            )
        except ValueError as exc:
            run.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
