from datetime import date
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer
from cognite.client.data_classes.data_modeling import ContainerId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import (
    MigrationCanvasCommand,
    MigrationPrepareCommand,
)
from cognite_toolkit._cdf_tk.commands._migrate import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMigrationIOAdapter,
    FileMetaIOAdapter,
    TimeSeriesIOAdapter,
)
from cognite_toolkit._cdf_tk.commands._migrate.creators import (
    InfieldV2ConfigCreator,
    InstanceSpaceCreator,
    SourceSystemCreator,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.selectors import (
    MigrateDataSetSelector,
    MigrationCSVFileSelector,
    MigrationSelector,
)
from cognite_toolkit._cdf_tk.storageio import AssetIO, EventIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.cli_args import parse_view_str
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    DataModelingSelect,
    ResourceViewMappingInteractiveSelect,
)
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricKind

TODAY = date.today()


class MigrateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("prepare")(self.prepare)
        self.command("data-sets")(self.data_sets)
        self.command("source-systems")(self.source_systems)
        self.command("assets")(self.assets)
        self.command("events")(self.events)
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
    def source_systems(
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
                multiselect=False,
                message="In which instance space do you want to create the source system?",
                include_empty=True,
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

    @classmethod
    def assets(
        cls,
        ctx: typer.Context,
        mapping_file: Annotated[
            Path | None,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Assets to CogniteAssets. "
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ] = None,
        data_set_id: Annotated[
            str | None,
            typer.Option(
                "--data-set-id",
                "-s",
                help="The data set ID to use for the migrated CogniteAssets. If not provided, the dataSetId from the mapping file is used. "
                "If neither is provided, the default data set for the project is used.",
            ),
        ] = None,
        ingestion_mapping: Annotated[
            str | None,
            typer.Option(
                "--ingestion-mapping",
                "-i",
                help="The ingestion mapping to use for the migrated assets. If not provided, "
                "the default mapping to CogniteAsset in CogniteCore will be used.",
            ),
        ] = None,
        consumption_view: Annotated[
            str | None,
            typer.Option(
                "--consumption-view",
                "-c",
                help="The consumption view to assign to the migrated assets Given as space:externalId/version. "
                "This will be used in Canvas to select which view to use when migrating assets. If not provided, "
                "CogniteAsset in CogniteCore will be used.",
            ),
        ] = None,
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
        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            verbose=verbose,
            kind="Assets",
            resource_type="asset",
            container_id=ContainerId("cdf_cdm", "CogniteAsset"),
        )

        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.migrate(
                selected=selected,
                data=AssetCentricMigrationIOAdapter(client, AssetIO(client)),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @staticmethod
    def _prepare_asset_centric_arguments(
        client: ToolkitClient,
        mapping_file: Path | None,
        data_set_id: str | None,
        consumption_view: str | None,
        ingestion_mapping: str | None,
        dry_run: bool,
        verbose: bool,
        kind: AssetCentricKind,
        resource_type: str,
        container_id: ContainerId,
    ) -> tuple[MigrationSelector, bool, bool]:
        if data_set_id is not None and mapping_file is not None:
            raise typer.BadParameter("Cannot specify both data_set_id and mapping_file")
        elif mapping_file is not None:
            selected: MigrationSelector = MigrationCSVFileSelector(datafile=mapping_file, kind=kind)
        elif data_set_id is not None:
            parsed_view = parse_view_str(consumption_view) if consumption_view is not None else None
            selected = MigrateDataSetSelector(
                data_set_external_id=data_set_id,
                kind=kind,
                ingestion_mapping=ingestion_mapping,
                preferred_consumer_view=parsed_view,
            )
        else:
            # Interactive selection of data set.
            selector = AssetInteractiveSelect(client, "migrate")
            selected_data_set_id = selector.select_data_set(allow_empty=False)
            asset_mapping = ResourceViewMappingInteractiveSelect(client, "migrate").select_resource_view_mapping(
                resource_type,
            )
            preferred_consumer_view = (
                DataModelingSelect(client, "migrate")
                .select_view(
                    multiselect=False,
                    include_global=True,
                    instance_type="node",
                    mapped_container=container_id,
                )
                .as_id()
            )
            selected = MigrateDataSetSelector(
                data_set_external_id=selected_data_set_id,
                kind=kind,
                ingestion_mapping=asset_mapping.external_id,
                preferred_consumer_view=preferred_consumer_view,
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).ask()
            if any(res is None for res in [dry_run, verbose]):
                raise typer.Abort()
        return selected, dry_run, verbose

    @classmethod
    def events(
        cls,
        ctx: typer.Context,
        mapping_file: Annotated[
            Path | None,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Events to CogniteActivity. "
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ] = None,
        data_set_id: Annotated[
            str | None,
            typer.Option(
                "--data-set-id",
                "-s",
                help="The data set ID to use for the migrated CogniteActivity. If not provided, the dataSetId from the mapping file is used. "
                "If neither is provided, the default data set for the project is used.",
            ),
        ] = None,
        ingestion_mapping: Annotated[
            str | None,
            typer.Option(
                "--ingestion-mapping",
                "-i",
                help="The ingestion mapping to use for the migrated events. If not provided, "
                "the default mapping to CogniteActivity in CogniteCore will be used.",
            ),
        ] = None,
        consumption_view: Annotated[
            str | None,
            typer.Option(
                "--consumption-view",
                "-c",
                help="The consumption view to assign to the migrated events Given as space:externalId/version. "
                "This will be used in Canvas to select which view to use when migrating events. If not provided, "
                "CogniteActivity in CogniteCore will be used.",
            ),
        ] = None,
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
        """Migrate Events to CogniteActivity."""
        client = EnvironmentVariables.create_from_environment().get_client()
        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            verbose=verbose,
            kind="Events",
            resource_type="event",
            container_id=ContainerId("cdf_cdm", "CogniteActivity"),
        )

        cmd = MigrationCommand()

        cmd.run(
            lambda: cmd.migrate(
                selected=selected,
                data=AssetCentricMigrationIOAdapter(client, EventIO(client)),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @classmethod
    def timeseries(
        cls,
        ctx: typer.Context,
        mapping_file: Annotated[
            Path | None,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from TimeSeries to CogniteTimeSeries. "
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ] = None,
        data_set_id: Annotated[
            str | None,
            typer.Option(
                "--data-set-id",
                "-s",
                help="The data set ID to select for the timeseries to migrate. If not provided and the mapping file is not provided"
                "an interactive selection will be performed to select the data set to migrate timeseries from.",
            ),
        ] = None,
        ingestion_mapping: Annotated[
            str | None,
            typer.Option(
                "--ingestion-mapping",
                "-i",
                help="The ingestion mapping to use for the migrated timeseries. If not provided, "
                "the default mapping to CogniteTimeSeries in CogniteCore will be used.",
            ),
        ] = None,
        consumption_view: Annotated[
            str | None,
            typer.Option(
                "--consumption-view",
                "-c",
                help="The consumption view to assign to the migrated timeseries Given as space:externalId/version. "
                "This will be used in Canvas to select which view to use when migrating timeseries. If not provided, "
                "CogniteTimeSeries in CogniteCore will be used.",
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where logs will be stored. If the directory does not exist, it will be created.",
            ),
        ] = Path(f"migration_logs_{TODAY!s}"),
        skip_linking: Annotated[
            bool,
            typer.Option(
                "--skip-linking",
                "-x",
                help="If set, the migration will not create links between the old TimeSeries and the new CogniteTimeSeries.",
            ),
        ] = False,
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

        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            verbose=verbose,
            kind="TimeSeries",
            resource_type="timeseries",
            container_id=ContainerId("cdf_cdm", "CogniteTimeSeries"),
        )
        if data_set_id is None and mapping_file is None:
            skip_linking = not questionary.confirm(
                "Do you want to link old and new TimeSeries?", default=not skip_linking
            ).ask()

        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.migrate(
                selected=selected,
                data=TimeSeriesIOAdapter(client, skip_linking=skip_linking),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    @classmethod
    def files(
        cls,
        ctx: typer.Context,
        mapping_file: Annotated[
            Path | None,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Files to CogniteFiles. "
                "This file is expected to have the following columns: [id, dataSetId, space, externalId]."
                "The dataSetId is optional, and can be skipped. If it is set, it is used to check the access to the dataset.",
            ),
        ] = None,
        data_set_id: Annotated[
            str | None,
            typer.Option(
                "--data-set-id",
                "-s",
                help="The data set ID to select for the files to migrate. If not provided, the dataSetId from the mapping file is used. "
                "If neither is provided, the default data set for the project is used.",
            ),
        ] = None,
        ingestion_mapping: Annotated[
            str | None,
            typer.Option(
                "--ingestion-mapping",
                "-i",
                help="The ingestion mapping to use for the migrated files. If not provided, "
                "the default mapping to CogniteFile in CogniteCore will be used.",
            ),
        ] = None,
        consumption_view: Annotated[
            str | None,
            typer.Option(
                "--consumption-view",
                "-c",
                help="The consumption view to assign to the migrated files Given as space:externalId/version. "
                "This will be used in Canvas to select which view to use when migrating timeseries. If not provided, "
                "CogniteFile in CogniteCore will be used.",
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where logs will be stored. If the directory does not exist, it will be created.",
            ),
        ] = Path(f"migration_logs_{TODAY!s}"),
        skip_linking: Annotated[
            bool,
            typer.Option(
                "--skip-linking",
                "-x",
                help="If set, the migration will not create links between the old FileMetadata and the new CogniteFile.",
            ),
        ] = False,
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

        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            verbose=verbose,
            kind="FileMetadata",
            resource_type="file",
            container_id=ContainerId("cdf_cdm", "CogniteFile"),
        )
        cmd = MigrationCommand()

        if data_set_id is None:
            skip_linking = not questionary.confirm(
                "Do you want to link old and new Files?", default=not skip_linking
            ).ask()

        cmd.run(
            lambda: cmd.migrate(
                selected=selected,
                data=FileMetaIOAdapter(client, skip_linking=skip_linking),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
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

    @staticmethod
    def infield_config(
        ctx: typer.Context,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Path to the directory where the Infield V2 configuration definitions will be dumped. It is recommended "
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
        """Creates Infield V2 configurations from existing APM Configurations in CDF."""
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = MigrationCommand()
        cmd.run(
            lambda: cmd.create(
                client,
                creator=InfieldV2ConfigCreator(client),
                output_dir=output_dir,
                dry_run=dry_run,
                verbose=verbose,
            )
        )
