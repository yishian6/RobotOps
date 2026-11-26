"""
    校验 wheel/sdist 的发布内容：必需资源、禁止文件和产物数量。

    该脚本由 CI 的 package job 与 Release 共用，避免两处检查规则漂移。
    只读取归档内容，不安装、不执行包内代码。
"""

from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

# 顶层或任意路径段出现这些目录/文件，都不应进入发布物。
FORBIDDEN_SEGMENTS = {".venv", ".git", "__pycache__", "build", "data", "doc"}


def archive_names(path: Path) -> list[str]:
    """
    返回 wheel 或 sdist 内的全部成员路径。

    Args:
        path: 已构建的 `.whl` 或 `.tar.gz` 文件。

    Returns:
        list[str]: 归一化后的 POSIX 风格成员路径。
    """
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    else:
        with tarfile.open(path, "r:gz") as archive:
            names = archive.getnames()
    return [name.replace("\\", "/") for name in names]


def forbidden(names: list[str]) -> list[str]:
    """
    按路径段找出环境文件、禁入目录和缓存。

    Args:
        names: 归档成员路径。

    Returns:
        list[str]: 所有命中的禁止成员；空列表表示通过。
    """
    return [
        name for name in names if any(
            segment in FORBIDDEN_SEGMENTS or segment.startswith(".env")
            for segment in PurePosixPath(name).parts
        )
    ]


def validate(path: Path) -> None:
    """
    校验单个归档的发布资源与禁止内容。

    Args:
        path: 待检查的 wheel 或 sdist。

    Returns:
        None: 校验失败时抛出 SystemExit，成功时打印摘要。
    """
    names = archive_names(path)
    if not any("prompts/investigation" in name for name in names):
        raise SystemExit(f"{path.name}: investigation prompts are missing")
    if not any("skills/ros1-topic-silence/SKILL.md" in name for name in names):
        raise SystemExit(f"{path.name}: ROS 1 Skill is missing")
    hits = forbidden(names)
    if hits:
        raise SystemExit(f"{path.name}: forbidden files: {hits[:5]}")
    print(f"{path.name}: packaged resources and layout are valid")


def main() -> int:
    """
    校验目录中恰好一份 wheel 和一份 sdist。

    Args:
        无显式参数；目录作为第一个命令行参数传入。

    Returns:
        int: 全部通过返回 0，失败由 SystemExit 表示非零退出。
    """
    if len(sys.argv) != 2:
        raise SystemExit("usage: validate_artifacts.py <artifact-directory>")
    directory = Path(sys.argv[1])
    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted(directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit(
            f"expected exactly one wheel and one sdist, got {len(wheels)} wheel(s) "
            f"and {len(sdists)} sdist(s)"
        )
    for package in (*wheels, *sdists):
        validate(package)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
