from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import AuthCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class AuthApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)
        self.command()(self.verify)

    def main(self, ctx: typer.Context) -> None:
        """Commands to auth setup"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf auth --help[/] for more information.")

    def init(
        self,
        no_verify: Annotated[
            bool,
            typer.Option(
                "--no-verify",
                "-v",
                help="Whether to skip the verification of the capabilities after the initialization.",
            ),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="If you verify, and you pass this flag no changes to CDF will be made.",
            ),
        ] = False,
    ) -> None:
        """Creates the authorization for a user/service principal to run the CDF Toolkit commands.

        This will prompt the user to log in and optionally store the credentials in a .env file.

        Needed capabilities for bootstrapping:
        "projectsAcl": ["LIST", "READ"],
        "groupsAcl": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"]
        """
        cmd = AuthCommand()
        cmd.run(
            lambda: cmd.init(
                no_verify=no_verify,
                dry_run=dry_run,
            )
        )

    def verify(
        self,
        ctx: typer.Context,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run. This means that no changes to CDF will be made.",
            ),
        ] = False,
    ) -> None:
        """Verify that the current user/service principal has the required capabilities to run the CDF Toolkit commands."""
        cmd = AuthCommand()

        cmd.run(
            lambda: cmd.verify(
                CDFToolConfig.from_context(ctx),
                dry_run=dry_run,
            )
        )
