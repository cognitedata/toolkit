"""This is the core functionality of the Cognite Data Fusion Toolkit."""

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Optional, Union

import typer
from dotenv import load_dotenv
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import BuildCommand, CleanCommand, DeployCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitValidationError,
)
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from cognite_toolkit._version import __version__ as current_version


# Common parameters handled in common callback
@dataclass
class Common:
    override_env: bool
    mockToolGlobals: Optional[CDFToolConfig]


CDF_TOML = CDFToml.load(Path.cwd())
_AVAILABLE_DATA_TYPES: tuple[str, ...] = tuple(LOADER_BY_FOLDER_NAME)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"CDF-Toolkit version: {current_version}.")
        raise typer.Exit()


class CoreApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.common)
        self.command()(self.build)
        self.command()(self.deploy)
        self.command()(self.clean)

    def common(
        self,
        ctx: typer.Context,
        override_env: Annotated[
            bool,
            typer.Option(
                help="Load the .env file in this or the parent directory, but also override currently set environment variables",
            ),
        ] = False,
        env_path: Annotated[
            Optional[str],
            typer.Option(
                help="Path to .env file to load. Defaults to .env in current or parent directory.",
            ),
        ] = None,
        version: Annotated[
            bool,
            typer.Option(
                "--version",
                help="See which version of the tooklit and the templates are installed.",
                callback=_version_callback,
            ),
        ] = False,
    ) -> None:
        """
        Docs: https://docs.cognite.com/cdf/deploy/cdf_toolkit/\n
        Template reference documentation: https://developer.cognite.com/sdks/toolkit/references/configs
        """
        if ctx.invoked_subcommand is None:
            print(
                Panel(
                    "\n".join(
                        [
                            "The Cognite Data Fusion Toolkit supports configuration of CDF projects from the command line or in CI/CD pipelines.",
                            "",
                            "[bold]Setup:[/]",
                            "1. Run [underline]cdf repo init[/] [italic]<directory name>[/] to set up a work directory.",
                            "2. Run [underline]cdf modules init[/] [italic]<directory name>[/] to initialise configuration modules.",
                            "",
                            "[bold]Configuration steps:[/]",
                            "3. Run [underline]cdf build[/] [italic]<directory name>[/] to verify the configuration for your project. Repeat for as many times as needed.",
                            "   Tip:[underline]cdf modules list[/] [italic]<directory name>[/] gives an overview of all your modules and their status.",
                            "",
                            "[bold]Deployment steps:[/]",
                            "4. Commit the [italic]<directory name>[/] to version control",
                            "5. Run [underline]cdf auth verify --interactive[/] to check that you have access to the relevant CDF project. ",
                            "    or [underline]cdf auth verify[/] if you have a .env file",
                            "6. Run [underline]cdf deploy --dry-run[/] to simulate the deployment of the configuration to the CDF project. Review the report provided.",
                            "7. Run [underline]cdf deploy[/] to deploy the configuration to the CDF project.",
                        ]
                    ),
                    title="Getting started",
                    style="green",
                    padding=(1, 2),
                )
            )
            return
        if override_env:
            print("  [bold yellow]WARNING:[/] Overriding environment variables with values from .env file...")

        if env_path is not None:
            if not (dotenv_file := Path(env_path)).is_file():
                raise ToolkitFileNotFoundError(env_path)

        else:
            if not (dotenv_file := Path.cwd() / ".env").is_file():
                if not (dotenv_file := Path.cwd().parent / ".env").is_file():
                    print("[bold yellow]WARNING:[/] No .env file found in current or parent directory.")

        if dotenv_file.is_file():
            has_loaded = load_dotenv(dotenv_file, override=override_env)
            if not has_loaded:
                print("  [bold yellow]WARNING:[/] No environment variables found in .env file.")

        ctx.obj = Common(
            override_env=override_env,
            mockToolGlobals=None,
        )

    def build(
        self,
        ctx: typer.Context,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        build_dir: Annotated[
            Path,
            typer.Option(
                "--build-dir",
                "-b",
                help="Where to save the built module files",
            ),
        ] = Path("./build"),
        selected: Annotated[
            Optional[list[str]],
            typer.Option(
                "--modules",
                "-m",
                help="Specify paths or names to the modules to build",
            ),
        ] = None,
        build_env_name: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                "-e",
                help="The name of the environment to build",
            ),
        ] = CDF_TOML.cdf.default_env,
        no_clean: Annotated[
            bool,
            typer.Option(
                "--no-clean",
                "-c",
                help="Whether not to delete the build directory before building the configurations",
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
        """Build configuration files from the modules to the build directory."""
        ToolGlobals: Union[CDFToolConfig, None] = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            # Remove the Error message from failing to load the config
            # This is verified in check_auth
            ToolGlobals = CDFToolConfig()

        cmd = BuildCommand()
        cmd.run(
            lambda: cmd.execute(
                verbose,
                organization_dir,
                build_dir,
                selected,
                build_env_name,
                no_clean,
                ToolGlobals,
            )
        )

    def deploy(
        self,
        ctx: typer.Context,
        build_dir: Annotated[
            str,
            typer.Argument(
                help="Where to find the module templates to deploy from. Defaults to current directory.",
                allow_dash=True,
            ),
        ] = "./build",
        build_env_name: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                "-e",
                help="CDF project environment to use for deployment. This is optional and "
                "if passed it is used to verify against the build environment",
            ),
        ] = None,
        drop: Annotated[
            bool,
            typer.Option(
                "--drop",
                "-d",
                help="Whether to drop existing configurations, drop per resource if present.",
            ),
        ] = False,
        drop_data: Annotated[
            bool,
            typer.Option(
                "--drop-data",
                help="Whether to drop existing data in data model containers and spaces.",
            ),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        include: Annotated[
            Optional[list[str]],
            typer.Option(
                "--include",
                help=f"Specify which resources to deploy, available options: {_AVAILABLE_DATA_TYPES}.",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Deploys the configuration files in the build directory to the CDF project."""
        cmd = DeployCommand(print_warning=True)
        include = _process_include(include)
        ToolGlobals = CDFToolConfig.from_context(ctx)
        cmd.run(
            lambda: cmd.execute(
                ToolGlobals,
                build_dir,
                build_env_name,
                dry_run,
                drop,
                drop_data,
                include,
                verbose,
            )
        )

    def clean(
        self,
        ctx: typer.Context,
        build_dir: Annotated[
            str,
            typer.Argument(
                help="Where to find the module templates to clean from. Defaults to ./build directory.",
                allow_dash=True,
            ),
        ] = "./build",
        build_env_name: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                "-e",
                help="CDF project environment to use for cleaning. This is optional and "
                "if passed it is used to verify against the build environment",
            ),
        ] = None,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present",
            ),
        ] = False,
        include: Annotated[
            Optional[list[str]],
            typer.Option(
                "--include",
                help=f"Specify which resource types to deploy, supported types: {_AVAILABLE_DATA_TYPES}",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Cleans the resources in the build directory from the CDF project."""
        # Override cluster and project from the options/env variables
        cmd = CleanCommand(print_warning=True)
        include = _process_include(include)
        ToolGlobals = CDFToolConfig.from_context(ctx)
        cmd.run(
            lambda: cmd.execute(
                ToolGlobals,
                build_dir,
                build_env_name,
                dry_run,
                include,
                verbose,
            )
        )


def _process_include(include: Optional[list[str]]) -> list[str]:
    if include and (invalid_types := set(include).difference(_AVAILABLE_DATA_TYPES)):
        raise ToolkitValidationError(
            f"Invalid resource types specified: {invalid_types}, available types: {_AVAILABLE_DATA_TYPES}"
        )
    include = include or list(_AVAILABLE_DATA_TYPES)
    return include
