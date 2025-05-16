from typing import Any

import typer
from rich import print


class ProfileApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("asset-centric")(self.asset_centric)

    def main(self, ctx: typer.Context) -> None:
        """Commands populate functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf profile --help[/] for more information.")

    @staticmethod
    def asset_centric(
        ctx: typer.Context,
        verbose: bool = False,
    ) -> None:
        """Asset centric profile"""
        raise NotImplementedError()
