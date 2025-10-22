from datetime import date
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer

from cognite_toolkit._cdf_tk.commands import (
    MigrateFilesCommand,
    MigrateTimeseriesCommand,
    MigrationCanvasCommand,
    MigrationPrepareCommand,
)
from cognite_toolkit._cdf_tk.commands._migrate import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.commands._migrate.creators import InstanceSpaceCreator, SourceSystemCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.storageio import AssetIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import AssetInteractiveSelect, DataModelingSelect

TODAY = date.today()


class MigrateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("prepare")(self.prepare)
        self.command("data-sets")(self.data_sets)
        self.command("source-system")(self.source_system)
        # Uncomment when command is ready.
        # self.command("assets")(self.assets)
        self.command("timeseries")(self.timeseries)
        self.command("files")(self.files)
        self.command("canvas")(self.canvas)

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
    def data_sets(
        ctx: typer.Context,
        data_set: Annotated[
            list[str] | None,
            typer.Argument(
                help="The name or external ID of the data set to create Instance Spaces for. If not provided, an "
                "interactive selection will be performed to select the data sets to create Instance Spaces for."
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Path to the directory where the instance space definitions will be dumped. It is recommended "
                "to govern these configurations in a git repository.",
            ),
        ] = Path("tmp"),
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
        """Creates Instance Spaces for all selected data sets."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_set is None:
            # Interactive model
            selector = AssetInteractiveSelect(client, "migrate")
            data_set = selector.select_data_sets()
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).ask()
            output_dir = questionary.path(
                "Specify output directory for instance space definitions:", default=str(output_dir)
            ).ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).ask()
            if any(res is None for res in [dry_run, output_dir, verbose]):
                raise typer.Abort()
            output_dir = Path(output_dir)

        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.create(
                client,
                creator=InstanceSpaceCreator(client, data_set_external_ids=data_set),
                output_dir=output_dir,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @staticmethod
    def source_system(
        ctx: typer.Context,
        data_set: Annotated[
            str | None,
            typer.Argument(
                help="The external ID of the data set to lookup source system for. If not provided, an interactive "
                "selection will be performed to select the data sets to create instance spaces for."
            ),
        ] = None,
        instance_space: Annotated[
            str | None,
            typer.Option(
                "--instance-space",
                "-s",
                help="The instance space were you want to create the source system.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Path to the directory where the instance space definitions will be dumped. It is recommended "
                "to govern these configurations in a git repository.",
            ),
        ] = Path("tmp"),
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
        """Creates source systems from the 'source' property of classic resources (assets, events, files)."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_set is None and instance_space is None:
            # Interactive model
            ds_selector = AssetInteractiveSelect(client, "migrate")
            data_set = ds_selector.select_data_set()
            dm_selector = DataModelingSelect(client, "migrate")
            instance_space = dm_selector.select_instance_space(
                multiselect=False, message="In which instance space do you want to create the source system?"
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).ask()
            output_dir = questionary.path(
                "Specify output directory for instance space definitions:", default=str(output_dir)
            ).ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).ask()
            if any(res is None for res in [instance_space, dry_run, output_dir, verbose]):
                raise typer.Abort()
            output_dir = Path(output_dir)
        elif data_set is None or instance_space is None:
            raise typer.BadParameter("Both data_set and instance_space must be provided together.")

        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.create(
                client,
                creator=SourceSystemCreator(client, data_set_external_id=data_set, instance_space=instance_space),
                output_dir=output_dir,
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
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ],
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where logs will be stored. If the directory does not exist, it will be created.",
            ),
        ] = Path(f"migration_logs_{TODAY!s}"),
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
        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.migrate(
                selected=MigrationCSVFileSelector(datafile=mapping_file, kind="asset"),
                data=AssetCentricMigrationIOAdapter(client, AssetIO(client)),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
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
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
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

    @staticmethod
    def files(
        ctx: typer.Context,
        mapping_file: Annotated[
            Path,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Files to CogniteFiles. "
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
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
        """Migrate Files to CogniteFiles."""

        client = EnvironmentVariables.create_from_environment().get_client(enable_set_pending_ids=True)
        cmd = MigrateFilesCommand()
        cmd.run(
            lambda: cmd.migrate_files(
                client,
                mapping_file=mapping_file,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @staticmethod
    def canvas(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID of the Canvas to migrate. If not provided, and interactive selection will be "
                "performed to select the Canvas to migrate."
            ),
        ] = None,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the migration will not be executed, but only a report of "
                "what would be done is printed. This is useful for checking that all resources referenced by the Canvas"
                "have been migrated to the new data modeling resources in CDF.",
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
        """Migrate Canvas applications from Asset-Centric to data modeling in CDF.

        This command expects that the CogniteMigration data model is already deployed, and that the Mapping view
        is populated with the mapping from Asset-Centric resources to the new data modeling resources.
        """
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = MigrationCanvasCommand()
        cmd.run(
            lambda: cmd.migrate_canvas(
                client,
                external_ids=external_id,
                dry_run=dry_run,
                verbose=verbose,
            )
        )
