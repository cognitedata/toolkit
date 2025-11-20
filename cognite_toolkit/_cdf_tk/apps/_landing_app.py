from typing import Annotated

import typer

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import InitCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags


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
        # TODO: this is a temporary solution to be able to test the functionality
        # in a new environment, assuming that the toml file doesn't exist yet.
        # remove this once v.07 is released
        v7: Annotated[
            bool,
            typer.Option(
                "--seven",
                "-s",
                help="Emulate v0.7",
                hidden=(Flags.v07.is_enabled() or not CDFToml.load().is_loaded_from_file),
            ),
        ] = False,
    ) -> None:
        """Getting started checklist"""
        cmd = InitCommand()
        # Tracking  command with the usual lambda run construct
        # is intentionally left out because we don't want to expose the user to the warning
        # before they've had the chance to opt in (which is something they'll do later using this command).
        cmd.execute(dry_run=dry_run, emulate_dot_seven=v7)
