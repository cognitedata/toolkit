from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._version import __version__


class ModulesApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)
        self.command()(self.upgrade)
        self.command()(self.list)

    def main(self, ctx: typer.Context) -> None:
        """Commands to manage modules"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf-tk modules --help[/] for more information.")

    def init(
        self,
        project_dir: Annotated[
            Optional[str],
            typer.Argument(
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        arg_package: Annotated[
            Optional[str],
            typer.Option(
                "--package",
                help="Name of package to include",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        cmd = ModulesCommand()
        cmd.run(
            lambda: cmd.init(
                init_dir=project_dir,
                arg_package=arg_package,
            )
        )

    def upgrade(
        self,
        project_dir: Annotated[
            Optional[str],
            typer.Argument(
                help="Directory path to project to upgrade with templates. Defaults to current directory.",
            ),
        ] = None,
    ) -> None:
        cmd = ModulesCommand()
        cmd.run(lambda: cmd.upgrade(project_dir=project_dir))

    # This is a trick to use an f-string for the docstring
    upgrade.__doc__ = f"""Upgrade the existing CDF project modules to version {__version__}."""

    def list(
        self,
        project_dir: Annotated[
            Path,
            typer.Argument(
                help="Directory path to project to list modules. Defaults to current directory.",
            ),
        ] = Path.cwd(),
        build_env: Annotated[
            str,
            typer.Option(
                "--env",
                help="Build environment to use. Defaults to 'dev'.",
            ),
        ] = "dev",
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Show more information.",
                is_flag=True,
            ),
        ] = False,
    ) -> None:
        cmd = ModulesCommand()
        cmd.run(lambda: cmd.list(project_dir=project_dir, build_env_name=build_env, verbose=verbose))
