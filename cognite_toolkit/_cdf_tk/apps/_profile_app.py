from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import ProfileCommand, ProfileRawCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class ProfileApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("asset-centric")(self.asset_centric)
        self.command("raw")(self.raw)

    def main(self, ctx: typer.Context) -> None:
        """Commands profile functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf profile --help[/] for more information.")

    @staticmethod
    def asset_centric(
        ctx: typer.Context,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the metadata and labels for each of the asset-centric resources.
        This shows an approximation of unstructured data count. This can, for example, be used to estimate the
        effort to model this data in data modeling."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileCommand()
        cmd.run(
            lambda: cmd.asset_centric(
                client,
                verbose,
            )
        )

    @staticmethod
    def raw(
        ctx: typer.Context,
        destination: Annotated[
            str,
            typer.Option(
                "--destination",
                "-d",
                help="Destination type the raw data should be written to. This can be 'assets', 'events', 'files',"
                "'timeseries', or 'sequences'.",
            ),
        ],
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the staging tables in CDF and where they are used.

        It works by checking all transformations that writes to the given destination and lists all the raw tables
        that are used in those transformations.

        This is intended to show the flow of data from raw into CDF. This can, for example, be used to determine the
        source of the data in a specific CDF resource.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileRawCommand()
        cmd.run(
            lambda: cmd.raw(
                client,
                destination,
                verbose,
            )
        )
