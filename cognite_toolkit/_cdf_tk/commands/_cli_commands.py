from __future__ import annotations

import shutil
import subprocess
from contextlib import suppress
from pathlib import Path


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
