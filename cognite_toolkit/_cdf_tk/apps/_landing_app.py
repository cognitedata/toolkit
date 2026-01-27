import contextlib
from typing import Annotated

import typer

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import InitCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class LandingApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)

    def main_init(
        self,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
    ) -> None:
        """Getting started checklist"""
        client: ToolkitClient | None = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            # Remove the Error message from failing to load the config
            # This is verified in check_auth
            client = EnvironmentVariables.create_from_environment().get_client()

        cmd = InitCommand(client=client)
        # Tracking  command with the usual lambda run construct
        # is intentionally left out because we don't want to expose the user to the warning
        # before they've had the chance to opt in (which is something they'll do later using this command).
        cmd.execute(dry_run=dry_run)
