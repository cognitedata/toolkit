from pathlib import Path
from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.commands import MigrateAssetsCommand, MigrateTimeseriesCommand, MigrationPrepareCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class MigrateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("prepare")(self.prepare)
        # Uncomment when command is ready.
        # self.command("assets")(self.assets)
        self.command("timeseries")(self.timeseries)

    def main(self, ctx: typer.Context) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf migrate --help[/] for more information.")

    @staticmethod
    def prepare(
        ctx: typer.Context,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="The preparation will not be executed, only report of what would be done is printed.",
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
        """Prepare the migration of resources from Asset-Centric to data modeling in CDF.

        This means deploying the CogniteMigration data model that contains the Mapping view. This will be used
        to store the mapping from Asset-Centric resources to the new data modeling resources.

        This mapping will be used when migrating applications such as Canvas, Charts, as well as resources that
        depend on the primary resources 3D and annotations.
        """
        client = EnvironmentVariables.create_from_environment().get_client(enable_set_pending_ids=True)
        cmd = MigrationPrepareCommand()
        cmd.run(
            lambda: cmd.deploy_cognite_migration(
                client,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @staticmethod
    def assets(
        ctx: typer.Context,
        mapping_file: Annotated[
            Path,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Assets to CogniteAssets. "
                "This file is expected to have the following columns: [id/externalId, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ],
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the migration will not be executed, but only a report of what would be done is printed.",
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
        """Migrate Assets to CogniteAssets."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = MigrateAssetsCommand()
        cmd.run(
            lambda: cmd.migrate_assets(
                client,
                mapping_file=mapping_file,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @staticmethod
    def timeseries(
        ctx: typer.Context,
        mapping_file: Annotated[
            Path,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from TimeSeries to CogniteTimeSeries. "
                "This file is expected to have the following columns: [id/externalId, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ],
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the migration will not be executed, but only a report of what would be done is printed.",
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
        """Migrate TimeSeries to CogniteTimeSeries."""

        client = EnvironmentVariables.create_from_environment().get_client(enable_set_pending_ids=True)
        cmd = MigrateTimeseriesCommand()
        cmd.run(
            lambda: cmd.migrate_timeseries(
                client,
                mapping_file=mapping_file,
                dry_run=dry_run,
                verbose=verbose,
            )
        )
