from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class PurgeApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("dataset")(self.purge_dataset)
        self.command("space")(self.purge_space)
        if Flags.PURGE_INSTANCES.is_enabled():
            self.command("instances")(self.purge_instances)

    def main(self, ctx: typer.Context) -> None:
        """Commands purge functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf purge --help[/] for more information.")

    def purge_dataset(
        self,
        ctx: typer.Context,
        external_id: Annotated[
            str | None,
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
            str | None,
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

    @staticmethod
    def purge_instances(
        view: Annotated[
            list[str] | None,
            typer.Argument(
                help="Purge instances with properties in the specified view. Expected format is "
                "'space externalId version'. For example 'cdf_cdm CogniteTimeSeries v1' will purge all nodes"
                "that have properties in the CogniteTimeSeries view. If not provided, interactive mode will be used.",
            ),
        ] = None,
        instance_space: Annotated[
            list[str] | None,
            typer.Option(
                "--instance-space",
                "-s",
                help="Only purge instances that are in the specified instance space(s).",
            ),
        ] = None,
        instance_type: Annotated[
            str,
            typer.Option(
                "--instance-type",
                "-t",
                help="Type of instances to purge. Can be 'node' or 'edge'. Default is 'node'.",
                case_sensitive=False,
                show_default=True,
            ),
        ] = "node",
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        unlink: Annotated[
            bool,
            typer.Option(
                "--skip-unlink",
                "-u",
                help="This only applies to CogniteTimeSeries and CogniteFile nodes. By default, the purge command will unlink the "
                "node from the datapoints/file content before deleting the node. If you want to delete the nodes with their datapoints/file content, "
                "you can skip the unlinking. Note that this will delete the datapoints/file content "
                "themselves, not the links to their parent nodes.",
            ),
        ] = True,
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="Automatically confirm that you are sure you want to purge the instances.",
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
        """This command will delete the contents of the specified instances."""

        cmd = PurgeCommand()
        client = EnvironmentVariables.create_from_environment().get_client(enable_set_pending_ids=True)
        cmd.run(
            lambda: cmd.instances(
                client,
                view,
                instance_space,
                instance_type,
                unlink=unlink,
                dry_run=dry_run,
                auto_yes=auto_yes,
                verbose=verbose,
            )
        )
