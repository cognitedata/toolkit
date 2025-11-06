from typing import Annotated

import typer

from cognite_toolkit._cdf_tk.commands import InitCommand


class LandingApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.main_init)

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
        cmd.run(lambda: cmd.execute(dry_run=dry_run))
