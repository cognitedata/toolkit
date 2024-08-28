from __future__ import annotations

import typer

from cognite_toolkit._cdf_tk.commands import InitCommand


class LandingApp(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.main_init)

    def main_init(self) -> None:
        """Guidance on how to get started"""
        cmd = InitCommand()
        cmd.run(cmd.execute)
