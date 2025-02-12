from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import PopulateCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class PopulateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("view")(self.populate_view)

    def main(self, ctx: typer.Context) -> None:
        """Commands populate functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf populate --help[/] for more information.")

    def populate_view(
        self,
        ctx: typer.Context,
        view_id: Annotated[
            Optional[list[str]],
            typer.Argument(
                help="View ID to populate. Format space external ID version, for example 'cdf_cdm CogniteAsset v1'"
                " If not provided, interactive mode will be used.",
            ),
        ] = None,
        table: Annotated[
            Optional[Path],
            typer.Option(
                "--table",
                "-t",
                help="Path to the file containing the data to populate the view with. This is required unless interactive"
                " mode is used",
            ),
        ] = None,
        instance_space: Annotated[
            Optional[str],
            typer.Option(
                "--space",
                "-s",
                help="The space to write the nodes to. This is required unless interactive mode is used",
            ),
        ] = None,
        external_id_column: Annotated[
            Optional[str],
            typer.Option(
                "--external-id-column",
                "-e",
                help="The name of the column in the table that contains the external IDs of the nodes. The column"
                " must be present in the table and must contain unique values. This is required unless interactive"
                " mode is used",
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
        """This command will populate a given view with data from a local table."""
        cmd = PopulateCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.view(
                client,
                view_id,
                table,
                instance_space,
                external_id_column,
                verbose,
            )
        )
