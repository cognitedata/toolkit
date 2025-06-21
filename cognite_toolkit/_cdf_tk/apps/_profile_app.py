from typing import Annotated, Any, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import ProfileAssetCentricCommand, ProfileAssetCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class ProfileApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("assets")(self.assets)
        self.command("asset-centric")(self.asset_centric)

    def main(self, ctx: typer.Context) -> None:
        """Commands profile functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf profile --help[/] for more information.")

    @staticmethod
    def assets(
        ctx: typer.Context,
        hierarchy: Annotated[
            Optional[str],
            typer.Option(
                "--hierarchy",
                "-h",
                help="The asset hierarchy to profile. This should be the externalId of the root asset. If not provided,"
                " ",
            ),
        ] = None,
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the assets in the given hierarchy.
        It works by listing all assets, events, files, timeseries, and sequences related to the given hierarchy.
        In addition, it lists the data sets that is used for each of the resources, the transformations that writes to
        these data sets, and the RAW tables that is used in these transformations..
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileAssetCommand()
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
        verbose: bool = False,
    ) -> None:
        """This command gives an overview over the metadata and labels for each of the asset-centric resources.
        This shows an approximation of unstructured data count. This can, for example, be used to estimate the
        effort to model this data in data modeling."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = ProfileAssetCentricCommand()
        cmd.run(
            lambda: cmd.asset_centric(
                client,
                verbose,
            )
        )
