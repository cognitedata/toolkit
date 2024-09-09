from __future__ import annotations

import shutil
import subprocess
from contextlib import suppress


class CLICommands:
    @classmethod
    def use_poetry(cls) -> bool:
        with suppress(Exception):
            return shutil.which("poetry") is not None
        return False

    @classmethod
    def use_git(cls) -> bool:
        with suppress(Exception):
            return shutil.which("git") is not None
        return False

    @classmethod
    def has_initiated_repo(cls) -> bool:
        with suppress(Exception):
            result = subprocess.run("git rev-parse --is-inside-work-tree".split(), stdout=subprocess.PIPE)
            return result.returncode == 0
        return False

    @classmethod
    def has_uncommitted_changes(cls) -> bool:
        with suppress(Exception):
            result = subprocess.run("git diff --quiet".split(), stdout=subprocess.PIPE)
            return result.returncode != 0
        return False
