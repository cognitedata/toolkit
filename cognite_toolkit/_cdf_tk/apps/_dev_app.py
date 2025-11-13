from typing import Any

import typer
from rich import print


class DevApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with development."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev --help[/] for more information.")
        return None
