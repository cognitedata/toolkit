from typing import Any

import typer


class MigrateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def main(self, ctx: typer.Context) -> None:
        """Commands migrate functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf migrate --help[/] for more information.")
