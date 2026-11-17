"""核心命令行入口，仅负责核心服务与 Prompt 配置检查。"""

import argparse
import json

from .agent.prompt_loader import load_investigation_prompt
from .config import Config


def main():
    """启动核心服务，或离线检查所选 Prompt。

    所有分支只依赖核心模块。可运行示例有自己的模块入口，核心不负责选择示例工厂、
    导入示例代码或分派其烟测命令，保持“示例依赖核心”的单向关系。
    """
    parser = argparse.ArgumentParser(description="RobotOps local runtime and investigation")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the local API")
    serve.add_argument("--port", type=int, default=8000)
    commands.add_parser(
        "check-prompts", help="Validate configured prompt resources without a model"
    )
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn

        uvicorn.run("robotops.api:create_app", factory=True, host="127.0.0.1", port=args.port)
    elif args.command == "check-prompts":
        config = Config.load()
        prompt = load_investigation_prompt(config.prompt_version, config.prompts_dir)
        print(json.dumps(prompt.metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
