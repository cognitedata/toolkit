from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import AuthCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


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
                "-nv",
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
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """Sets the OIDC parameters required to authenticate and authorize the Cognite Toolkit in Cognite Data Fusion.

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
        no_prompt: Annotated[
            bool,
            typer.Option(
                "--no-prompt",
                "-np",
                help="Whether to skip the prompt to continue. This is useful for CI/CD pipelines."
                "If you include this flag, the execution will stop if the user or service principal does not have the required capabilities.",
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
        """Verify that the current user or service principal has the required capabilities to run the CDF Toolkit commands."""
        cmd = AuthCommand()
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd.run(
            lambda: cmd.verify(
                client,
                dry_run=dry_run,
                no_prompt=no_prompt,
            )
        )
