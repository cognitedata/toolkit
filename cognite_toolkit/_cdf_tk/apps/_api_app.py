from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.commands import FunctionServiceCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from ._helpers import print_help_if_no_subcommand


class FunctionsApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.activate)

    def main(self, ctx: typer.Context) -> None:
        """CDF Function service commands."""
        print_help_if_no_subcommand(ctx)

    def activate(
        self,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run. This means that no changes to CDF will be made.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Activate the CDF Function service in the project (may take up to 2 hours)."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = FunctionServiceCommand()
        cmd.run(lambda: cmd.activate(client, dry_run=dry_run))


class ApiApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(FunctionsApp(), name="functions")

    def main(self, ctx: typer.Context) -> None:
        """Direct CDF API actions."""
        print_help_if_no_subcommand(ctx)
