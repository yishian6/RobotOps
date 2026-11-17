"""pipeline 示例的独立入口：python -m robotops_examples.pipeline。"""

import argparse
import json

from .smoke import smoke_check


def main():
    """分派本示例的服务与烟测，不向核心 CLI 注册场景命令。

    serve 组装带 /demo 控制路由的示例应用；smoke 在临时目录运行真实故障检查，
    返回观测汇总并清理子进程。两者都通过明确的示例入口调用。
    """
    parser = argparse.ArgumentParser(description="RobotOps pipeline example")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the pipeline example API")
    serve.add_argument("--port", type=int, default=8000)
    commands.add_parser("smoke", help="Check real pipeline faults without calling a model")
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn

        uvicorn.run(
            "robotops_examples.pipeline.app:create_app",
            factory=True,
            host="127.0.0.1",
            port=args.port,
        )
    else:
        print(json.dumps(smoke_check(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
