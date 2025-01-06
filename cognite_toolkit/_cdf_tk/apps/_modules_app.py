from pathlib import Path
from typing import Annotated, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import ModulesCommand, PullCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from cognite_toolkit._version import __version__

CDF_TOML = CDFToml.load(Path.cwd())


class ModulesApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)
        self.command()(self.upgrade)
        if Flags.MODULE_PULL.is_enabled():
            self.command()(self.pull)
        self.command()(self.list)
        self.command()(self.add)

    def main(self, ctx: typer.Context) -> None:
        """Commands to manage modules"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf modules --help[/] for more information.")

    def init(
        self,
        organization_dir: Annotated[
            Optional[Path],
            typer.Argument(
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        all: Annotated[
            bool,
            typer.Option(
                "--all",
                "-a",
                help="Copy all available templates.",
            ),
        ] = False,
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-a",
                help="Clean target directory if it exists",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        cmd = ModulesCommand()
        cmd.run(
            lambda: cmd.init(
                organization_dir=organization_dir,
                select_all=all,
                clean=clean,
            )
        )

    def upgrade(
        self,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Print details of each change applied in the upgrade process.",
            ),
        ] = False,
    ) -> None:
        cmd = ModulesCommand()
        cmd.run(lambda: cmd.upgrade(organization_dir=organization_dir, verbose=verbose))

    # This is a trick to use an f-string for the docstring
    upgrade.__doc__ = f"""Upgrade the existing CDF project modules to version {__version__}."""

    def add(
        self,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to project directory with the modules. This is used to search for available functions.",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Add one or more new module(s) to the project."""
        cmd = ModulesCommand()
        cmd.run(lambda: cmd.add(organization_dir=organization_dir))

    def pull(
        self,
        ctx: typer.Context,
        module: Annotated[
            str,
            typer.Argument(
                help="The module or path to module to pull from CDF.",
                allow_dash=True,
            ),
        ],
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        build_env: Annotated[
            str,
            typer.Option(
                "--env",
                "-e",
                help="Build environment to use.",
            ),
        ] = CDF_TOML.cdf.default_env,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="Do now change the local files on the disk.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Print details of each change applied in the pull process.",
            ),
        ] = False,
    ) -> None:
        """Pull a module from CDF. This will overwrite the local files with the latest version from CDF."""
        cmd = PullCommand()
        ToolGlobals = CDFToolConfig.from_context(ctx)
        cmd.run(
            lambda: cmd.pull_module(
                module=module,
                organization_dir=organization_dir,
                env=build_env,
                dry_run=dry_run,
                verbose=verbose,
                ToolGlobals=ToolGlobals,
            )
        )

    def list(
        self,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        build_env: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                help="Build environment to use.",
            ),
        ] = CDF_TOML.cdf.default_env,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """List all available modules in the project."""
        cmd = ModulesCommand()
        cmd.run(lambda: cmd.list(organization_dir=organization_dir, build_env_name=build_env))
