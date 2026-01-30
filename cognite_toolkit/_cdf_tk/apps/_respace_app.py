from typing import Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import RespaceCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class RespaceApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("plan")(self.respace_plan)
        self.command("execute")(self.respace_execute)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands for respacing (moving) nodes between CDF spaces."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf data respace --help[/] for more information.")

    @staticmethod
    def respace_plan() -> None:
        """Generate a respace plan by analyzing nodes and their dependencies."""
        cmd = RespaceCommand()
        cmd.run(lambda: cmd.plan())

    @staticmethod
    def respace_execute() -> None:
        """Execute a respace plan, migrating nodes between spaces."""
        cmd = RespaceCommand()
        cmd.run(lambda: cmd.execute())
