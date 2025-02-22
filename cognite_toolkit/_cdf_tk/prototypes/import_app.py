from pathlib import Path

import typer

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from .commands.import_ import ImportTransformationCLI

import_app = typer.Typer(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
)


@import_app.callback(invoke_without_command=True)
def import_main(ctx: typer.Context) -> None:
    """PREVIEW FEATURE Import resources into Cognite-Toolkit."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk import --help[/] for more information.")
    return None


@import_app.command("transformation-cli")
def transformation_cli(
    source: Path = typer.Argument(..., help="Path to the transformation CLI manifest directory or files."),
    destination: Path = typer.Argument(..., help="Path to the destination directory."),
    overwrite: bool = typer.Option(False, help="Overwrite destination if it already exists."),
    flatten: bool = typer.Option(False, help="Flatten the directory structure."),
    clean: bool = typer.Option(False, help="Remove the source directory after import."),
    verbose: bool = typer.Option(False, help="Turn on to get more verbose output when running the command"),
) -> None:
    """Import transformation CLI manifests into Cognite-Toolkit modules."""

    # We are lazy loading the client as we only need it if we need to look up dataset ids.
    # This is to ensure the command can be executed without a client if the user does not need to look up dataset ids.
    # (which is likely 99% of the time)
    def get_client() -> ToolkitClient:
        return EnvironmentVariables.create_from_environment().get_client()

    cmd = ImportTransformationCLI(print_warning=True, get_client=get_client)
    cmd.execute(source, destination, overwrite, flatten, clean, verbose=verbose)
