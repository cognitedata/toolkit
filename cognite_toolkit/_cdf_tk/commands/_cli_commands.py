import os
import shutil
import subprocess
from contextlib import suppress
from pathlib import Path


def use_uv() -> bool:
    """Return True when the toolkit is running under uv (e.g. ``uv run cdf build``)."""
    return os.environ.get("UV") is not None


def package_install_command(package: str) -> str:
    """Return the preferred install command for the current Python environment manager."""
    if use_uv():
        return f"uv add {package}"
    return f"pip install {package}"


def use_poetry() -> bool:
    with suppress(Exception):
        return shutil.which("poetry") is not None
    return False


def use_git() -> bool:
    with suppress(Exception):
        return shutil.which("git") is not None
    return False


def has_initiated_repo() -> bool:
    with suppress(Exception):
        result = subprocess.run("git rev-parse --is-inside-work-tree".split(), stdout=subprocess.PIPE)
        return result.returncode == 0
    return False


def has_uncommitted_changes() -> bool:
    with suppress(Exception):
        result = subprocess.run("git diff --quiet".split(), stdout=subprocess.PIPE)
        return result.returncode != 0
    return False


def git_root() -> Path | None:
    with suppress(Exception):
        result = subprocess.run("git rev-parse --show-toplevel".split(), stdout=subprocess.PIPE)
        return Path(result.stdout.decode(encoding="utf-8").strip())
    return None
