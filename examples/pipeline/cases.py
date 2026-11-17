"""显式导出示例检查手册；不会在核心启动或每次调查时自动注入已知故障答案。"""

import json
from importlib.resources import files


def main():
    """逐行输出可 POST /cases 的 JSON，来源标为 smoke fixture，不冒充生产历史。"""
    cases = json.loads(files("robotops_examples.pipeline").joinpath("cases.json").read_text())
    for case in cases:
        print(json.dumps(case, ensure_ascii=False))


if __name__ == "__main__":
    main()
