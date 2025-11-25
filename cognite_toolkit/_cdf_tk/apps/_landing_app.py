from typing import Annotated

import typer

from cognite_toolkit._cdf_tk.commands import InitCommand


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
        cmd = InitCommand()
        # Tracking  command with the usual lambda run construct
        # is intentionally left out because we don't want to expose the user to the warning
        # before they've had the chance to opt in (which is something they'll do later using this command).
        cmd.execute(dry_run=dry_run)
