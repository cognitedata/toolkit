import shutil
import sys
import venv
from pathlib import Path
from subprocess import Popen
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
        args = [str(Path(context.bin_path) / "pip"), "install", "-r", "requirements.txt"]

        function_dir = Path(context.env_dir)
        requirements_destination_path = function_dir / "requirements.txt"
        if isinstance(self.requirements_txt, Path):
            shutil.copy(self.requirements_txt, requirements_destination_path)
        else:
            requirements_destination_path.write_text(self.requirements_txt, encoding="utf-8")

        process = Popen(args, stdout=sys.stdout, stderr=sys.stderr, cwd=str(function_dir))
        process.wait()
        if process.returncode != 0:
            suffix = f" {self.requirements_txt.as_posix()}" if isinstance(self.requirements_txt, Path) else ""
            raise ToolkitEnvError(f"Invalid 'requirements.txt' file{suffix}.")
        self._context = context

    def check_import(self) -> None: ...

    def run(self, environment: dict[str, str]) -> None: ...
