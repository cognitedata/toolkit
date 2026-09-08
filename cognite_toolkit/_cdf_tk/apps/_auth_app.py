from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.commands import AuthCommand
from cognite_toolkit._cdf_tk.commands.auth_session import AuthSessionCommand
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.utils.auth import VALID_AUTH_LOGIN_FLOWS, AuthLoginFlowCli, EnvironmentVariables

from ._helpers import print_help_if_no_subcommand


class AuthApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)
        self.command()(self.verify)
        if FeatureFlag.is_enabled(Flags.V09):
            self.command()(self.login)
            self.command()(self.logout)
            self.command()(self.status)

    def main(self, ctx: typer.Context) -> None:
        """Commands to auth setup"""
        print_help_if_no_subcommand(ctx)

    def init(
        self,
        reset: Annotated[
            bool,
            typer.Option(
                "--reset",
                help="Start over from scratch, ignoring any existing environment variables. "
                "An existing .env file will be backed up before being overwritten.",
                hidden=True,
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
        # We do not pass in a client here as this is typically used to create the .env file needed for authentication.
        cmd = AuthCommand()
        cmd.run(lambda: cmd.init(reset=reset))

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
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = AuthCommand(client=client)
        cmd.run(
            lambda: cmd.verify(
                client,
                dry_run=dry_run,
                no_prompt=no_prompt,
            )
        )

    def login(
        self,
        flow: Annotated[
            AuthLoginFlowCli,
            typer.Option(
                "--flow",
                "-f",
                help="Authentication flow to use.",
                case_sensitive=False,
            ),
        ] = "session",
        org: Annotated[
            str | None,
            typer.Option("--org", "-o", help="Organization to sign in to when using the session flow"),
        ] = None,
        force: Annotated[
            bool,
            typer.Option("--force", help="Replace an existing session without prompting"),
        ] = False,
        port: Annotated[
            int | None,
            typer.Option(
                "--port",
                "-p",
                help="Local callback port for the OAuth redirect (default: 3000, session flow only)",
            ),
        ] = None,
    ) -> None:
        """Sign in and optionally write a .env file for subsequent Toolkit commands."""
        if flow not in VALID_AUTH_LOGIN_FLOWS:
            raise typer.BadParameter(f"Invalid flow {flow!r}. Choose one of: {', '.join(VALID_AUTH_LOGIN_FLOWS)}")

        if flow != "session":
            session_only_flags = [
                flag
                for flag, is_set in (("--org", org is not None), ("--force", force), ("--port", port is not None))
                if is_set
            ]
            if session_only_flags:
                raise typer.BadParameter(f"{', '.join(session_only_flags)} are only valid with --flow session")
            org = None
            force = False
            port = None

        cmd = AuthCommand()
        cmd.run(
            lambda: cmd.login(
                flow=flow,
                org=org,
                force=force,
                port=port,
            )
        )

    def logout(self) -> None:
        """Sign out and clear the persisted CogIdP session."""
        cmd = AuthCommand()
        cmd.run(AuthSessionCommand().logout)

    def status(self) -> None:
        """Show the current persisted CogIdP session."""
        cmd = AuthCommand()
        cmd.run(AuthSessionCommand().status)
