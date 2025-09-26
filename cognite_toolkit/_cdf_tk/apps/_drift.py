from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands.drift import DriftCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

CDF_TOML = CDFToml.load(Path.cwd())


class DriftApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("transformations")(self.drift_transformations)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to sync resources between UI and local project."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf drift --help[/] for more information.")

    @staticmethod
    def drift_transformations(
        ctx: typer.Context,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to project directory with the modules. Defaults to the current working directory.",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        env_name: Annotated[
            str | None,
            typer.Option(
                "--env",
                "-e",
                help="Name of the build environment to use. Defaults to the current environment.",
            ),
        ] = CDF_TOML.cdf.default_env,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Run without making changes.",
            ),
        ] = False,
        yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="Auto-accept prompts.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Verbose output.",
            ),
        ] = False,
    ) -> None:
        """Commands to sync transformations between UI and local project."""
        cmd = DriftCommand()
        env_vars = EnvironmentVariables.create_from_environment()
        cmd.run(lambda: cmd.run_transformations(env_vars, organization_dir, env_name, dry_run, yes, verbose))
