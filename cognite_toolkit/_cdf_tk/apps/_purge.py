from typing import Annotated, Any, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class PurgeApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("dataset")(self.purge_dataset)
        self.command("space")(self.purge_space)

    def main(self, ctx: typer.Context) -> None:
        """Commands purge functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf purge --help[/] for more information.")

    def purge_dataset(
        self,
        ctx: typer.Context,
        external_id: Annotated[
            Optional[str],
            typer.Argument(
                help="External id of the dataset to purge. If not provided, interactive mode will be used.",
            ),
        ] = None,
        include_dataset: Annotated[
            bool,
            typer.Option(
                "--include-dataset",
                "-i",
                help="Include dataset in the purge. This will also archive the dataset.",
            ),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="Automatically confirm that you are sure you want to purge the dataset.",
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
        """This command will delete the contents of the specified dataset"""
        cmd = PurgeCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.dataset(
                client,
                external_id,
                include_dataset,
                dry_run,
                auto_yes,
                verbose,
            )
        )

    def purge_space(
        self,
        ctx: typer.Context,
        space: Annotated[
            Optional[str],
            typer.Argument(
                help="Space to purge. If not provided, interactive mode will be used.",
            ),
        ] = None,
        include_space: Annotated[
            bool,
            typer.Option(
                "--include-space",
                "-i",
                help="Include space in the purge. This will also delete the space.",
            ),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="Automatically confirm that you are sure you want to purge the space.",
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
        """This command will delete the contents of the specified space."""

        cmd = PurgeCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.space(
                client,
                space,
                include_space,
                dry_run,
                auto_yes,
                verbose,
            )
        )
