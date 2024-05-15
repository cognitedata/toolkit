from pathlib import Path

import typer
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.templates import (
    build_config,
)
from cognite_toolkit._cdf_tk.templates.data_classes import (
    BuildConfigYAML,
    SystemYAML,
)

from ._commands import ToolkitCommand


class BuildCommand(ToolkitCommand):
    def execute(
        self, ctx: typer.Context, source_path: Path, build_dir: Path, build_env_name: str, no_clean: bool
    ) -> None:
        if not source_path.is_dir():
            raise ToolkitNotADirectoryError(str(source_path))

        system_config = SystemYAML.load_from_directory(source_path, build_env_name, self.warn)
        config = BuildConfigYAML.load_from_directory(source_path, build_env_name, self.warn)
        print(
            Panel(
                f"[bold]Building config files from templates into {build_dir!s} for environment {build_env_name} using {source_path!s} as sources...[/bold]"
                f"\n[bold]Config file:[/] '{config.filepath.absolute()!s}'"
            )
        )
        config.set_environment_variables()

        build_config(
            build_dir=Path(build_dir),
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=not no_clean,
            verbose=ctx.obj.verbose,
        )
