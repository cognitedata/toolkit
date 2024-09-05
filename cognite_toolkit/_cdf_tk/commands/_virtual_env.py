from __future__ import annotations

import shutil
import sys
import venv
from collections.abc import Mapping
from pathlib import Path
from subprocess import DEVNULL, Popen
from types import SimpleNamespace

from cognite_toolkit._cdf_tk.exceptions import ToolkitEnvError


class FunctionVirtualEnvironment(venv.EnvBuilder):
    def __init__(self, requirements_txt: Path | str, rebuild: bool) -> None:
        super().__init__(
            system_site_packages=False,
            clear=rebuild,
            with_pip=True,
        )
        self.requirements_txt = requirements_txt
        self._context: SimpleNamespace | None = None

    def post_setup(self, context: SimpleNamespace) -> None:
        args = [str(Path(context.bin_path) / "pip"), "install", "--disable-pip-version-check", "-r", "requirements.txt"]

        function_dir = Path(context.env_dir).parent
        requirements_destination_path = function_dir / "requirements.txt"
        if isinstance(self.requirements_txt, Path):
            shutil.copy(self.requirements_txt, requirements_destination_path)
        else:
            requirements_destination_path.write_text(self.requirements_txt, encoding="utf-8")

        process = Popen(args, stdout=DEVNULL, stderr=sys.stderr, cwd=str(function_dir))
        process.wait()
        if process.returncode != 0:
            suffix = f" {self.requirements_txt.as_posix()}" if isinstance(self.requirements_txt, Path) else ""
            raise ToolkitEnvError(
                f"Unable to install dependencies in 'requirements.txt' in the current environment {suffix}."
            )
        self._context = context

    def execute(self, script: Path, script_name: str, env: Mapping[str, str] | None = None) -> None:
        if self._context is None:
            raise ToolkitEnvError("Virtual environment not created.")
        function_dir = Path(self._context.env_dir).parent
        args = [str(Path(self._context.bin_path) / "python"), str(script)]

        process = Popen(args, stdout=sys.stdout, stderr=sys.stderr, cwd=str(function_dir), env=env)
        process.wait()
        if process.returncode != 0:
            raise ToolkitEnvError(f"Error executing {script_name} {script.as_posix()}.")
