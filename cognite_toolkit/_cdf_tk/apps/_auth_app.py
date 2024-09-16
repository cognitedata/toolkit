from typing import Any

import typer
from rich import print


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
        verbose: bool = typer.Option(False, "-v", "--verbose", help="Verbose output"),
    ) -> None:
        """Creates the authorization for a user/service principal to run the CDF Toolkit commands.

        This will prompt the user to log in and optionally store the credentials in a .env file.

        Needed capabilities for bootstrapping:
        "projectsAcl": ["LIST", "READ"],
        "groupsAcl": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"]
        """
        print("Auth setup")

    def verify(
        self,
    ) -> None:
        """Verify that the current user/service principal has the required capabilities to run the CDF Toolkit commands."""
        print("Auth verify")
