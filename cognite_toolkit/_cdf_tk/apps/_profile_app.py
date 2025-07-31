from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import (
    ProfileAssetCentricCommand,
    ProfileAssetCommand,
    ProfileRawCommand,
    ProfileTransformationCommand,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class ProfileApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("assets")(self.assets)
        self.command("asset-centric")(self.asset_centric)
        self.command("transformations")(self.transformations)
        self.command("raw")(self.raw)

    def main(self, ctx: typer.Context) -> None:
        """Commands profile functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf profile --help[/] for more information.")

    @staticmethod
    def assets(
        ctx: typer.Context,
        hierarchy: Annotated[
            str | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="The asset hierarchy to profile. This should be the externalId of the root asset. If not provided,"
                " an interactive prompt will be used to select the hierarchy.",
            ),
        ] = None,
        output_spreadsheet: Annotated[
            Path | None,
            typer.Option(
                "--output-spreadsheet",
                "-o",
                help="The path to the output spreadsheet. If not provided, the output will only be printed to the console.",
            ),
        ] = None,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the assets in the given hierarchy.
        It works by listing all assets, events, files, timeseries, and sequences related to the given hierarchy.
        In addition, it lists the data sets that is used for each of the resources, the transformations that writes to
        these data sets, and the RAW tables that is used in these transformations.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileAssetCommand(output_spreadsheet)
        cmd.run(
            lambda: cmd.assets(
                client,
                hierarchy,
                verbose,
            )
        )

    @staticmethod
    def asset_centric(
        ctx: typer.Context,
        hierarchy: Annotated[
            str | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="The asset hierarchy to profile. This should be the externalId of the root asset. If not provided,"
                " an interactive prompt will be used to select the hierarchy (or select all assets-centric assets).",
            ),
        ] = None,
        output_spreadsheet: Annotated[
            Path | None,
            typer.Option(
                "--output-spreadsheet",
                "-o",
                help="The path to the output spreadsheet. If not provided, the output will only be printed to the console.",
            ),
        ] = None,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the metadata and labels for each of the asset-centric resources.
        This shows an approximation of unstructured data count. This can, for example, be used to estimate the
        effort to model this data in data modeling."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileAssetCentricCommand(output_spreadsheet)
        cmd.run(
            lambda: cmd.asset_centric(
                client,
                hierarchy,
                verbose,
            )
        )

    @staticmethod
    def transformations(
        destination: Annotated[
            str | None,
            typer.Option(
                "--destination",
                "-d",
                help="Destination type the transformations data should be written to. This can be 'assets', 'events', 'files',"
                "'timeseries', or 'sequences'. If not provided, and interactive mode is enabled, the user will be prompted to select a destination.",
            ),
        ] = None,
        output_spreadsheet: Annotated[
            Path | None,
            typer.Option(
                "--output-spreadsheet",
                "-o",
                help="The path to the output spreadsheet. If not provided, the output will only be printed to the console.",
            ),
        ] = None,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the transformations that write to the given destination.
        It works by checking all transformations that writes to the given destination, lists the sources of the data,
        and the target columns.
        This is intended to show the flow of data from raw into CDF. This can, for example, be used to determine the
        source of the data in a specific CDF resource.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileTransformationCommand(output_spreadsheet)
        cmd.run(
            lambda: cmd.transformation(
                client,
                destination,
                verbose,
            )
        )

    @staticmethod
    def raw(
        ctx: typer.Context,
        destination: Annotated[
            str | None,
            typer.Option(
                "--destination",
                "-d",
                help="Destination type the raw data should be written to. This can be 'assets', 'events', 'files',"
                "'timeseries', or 'sequences'. If not provided, and interactive mode is enabled, the user will be prompted to select a destination.",
            ),
        ] = None,
        output_spreadsheet: Annotated[
            Path | None,
            typer.Option(
                "--output-spreadsheet",
                "-o",
                help="The path to the output spreadsheet. If not provided, the output will only be printed to the console.",
            ),
        ] = None,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the staging tables in CDF and where they are used.
        It works by checking all transformations that writes to the given destination and lists all the raw tables
        that are used in those transformations.
        This is intended to show the flow of data from raw into CDF. This can, for example, be used to determine the
        source of the data in a specific CDF resource.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileRawCommand(output_spreadsheet)
        cmd.run(
            lambda: cmd.raw(
                client,
                destination,
                verbose,
            )
        )
