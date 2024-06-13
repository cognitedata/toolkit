from __future__ import annotations

from typing import Annotated, Optional

import typer

from cognite_toolkit._cdf_tk.prototypes.init import InitCommand


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def interactive(
        self,
        ctx: typer.Context,
        arg_init_dir: Annotated[
            Optional[str],
            typer.Option(
                "--init-dir",
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        arg_package: Annotated[
            Optional[str],
            typer.Option(
                "--package",
                help="Name of package to include",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        cmd = InitCommand()
        cmd.run(
            ctx,
            init_dir=arg_init_dir,
            arg_package=arg_package,
        )


command = InteractiveInit(
    name="init", help="Initialize or upgrade a new CDF project with templates interactively."
).interactive
