from typing import Annotated

import typer

from cognite_toolkit._cdf_tk.commands import InitCommand


class LandingApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        # Command is registered directly in _cdf.py, not here

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
        v7: Annotated[
            bool,
            typer.Option("--seven", "-s", help="Emulate v0.7", hidden=False),
        ] = False,
    ) -> None:
        """Getting started checklist"""
        cmd = InitCommand()
        # do not track the command with the usual lambda run
        # construct because we don't want to display the warning message here
        cmd.execute(dry_run=dry_run, emulate_dot_seven=v7)
