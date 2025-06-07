from typing import Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import ProfileCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class ProfileApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("asset-centric")(self.asset_centric)

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
