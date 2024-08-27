from __future__ import annotations

from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import (
    RunTransformationCommand,
)
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class RunApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("transformation")(self.run_transformation)
        self.add_typer(RunFunctionApp(*args, **kwargs), name="function")

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to execute processes in CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf run --help[/] for more information.")

    @staticmethod
    def run_transformation(
        ctx: typer.Context,
        external_id: Annotated[
            str,
            typer.Option(
                "--external-id",
                "-e",
                prompt=True,
                help="External id of the transformation to run.",
            ),
        ],
    ) -> None:
        """This command will run the specified transformation using a one-time session."""
        cmd = RunTransformationCommand()
        cmd.run(lambda: cmd.run_transformation(CDFToolConfig.from_context(ctx), external_id))


class RunFunctionApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.command("local")(self.run_local)
        self.command("cdf")(self.run_cdf)

    @staticmethod
    def run_local(
        ctx: typer.Context,
        external_id: Annotated[
            str,
            typer.Option(
                "--external-id",
                "-e",
                prompt=True,
                help="External id of the function to run.",
            ),
        ],
    ) -> None:
        """This command will run the specified function locally."""
        print(f"Running function with external id: {external_id}")

    @staticmethod
    def run_cdf(
        ctx: typer.Context,
        external_id: Annotated[
            str,
            typer.Option(
                "--external-id",
                "-e",
                prompt=True,
                help="External id of the function to run.",
            ),
        ],
    ) -> None:
        """This command will run the specified function (assuming it is deployed) in CDF."""
        print(f"Running function with external id: {external_id} in CDF.")
