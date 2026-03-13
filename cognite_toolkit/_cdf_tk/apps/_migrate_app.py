from datetime import date
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ContainerId
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands import MigrationPrepareCommand
from cognite_toolkit._cdf_tk.commands._migrate import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    ConnectionCreator,
    InFieldAssetMapping,
    InFieldConditionMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.creators import (
    InfieldV2ConfigCreator,
    InstanceSpaceCreator,
    SourceSystemCreator,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
    AssetCentricMapper,
    CanvasMapper,
    ChartMapper,
    FDMtoCDMMapper,
    InFieldLegacyToCDMScheduleMapper,
    RecordsMapper,
    ThreeDAssetMapper,
    ThreeDMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    create_infield_data_mappings,
    create_infield_schedule_selector,
)
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
    RecordsMigrationIO,
    ThreeDAssetMappingMigrationIO,
    ThreeDMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import (
    AssetCentricMigrationSelector,
    MigrateDataSetSelector,
    MigrationCSVFileSelector,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.storageio import CanvasIO, ChartIO, InstanceIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    CanvasExternalIdSelector,
    ChartExternalIdSelector,
    InstanceQuerySelector,
    InstanceViewSelector,
    SelectedView,
    ThreeDModelIdSelector,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.cli_args import parse_view_str
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    APMConfigInteractiveSelect,
    AssetInteractiveSelect,
    DataModelingSelect,
    FileMetadataInteractiveSelect,
    InteractiveCanvasSelect,
    InteractiveChartSelect,
    ResourceViewMappingInteractiveSelect,
    ThreeDInteractiveSelect,
    ViewSelectFilter,
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
        self.command("annotations")(self.annotations)
        self.command("canvas")(self.canvas)
        self.command("charts")(self.charts)
        self.command("3d")(self.three_d)
        self.command("3d-mappings")(self.three_d_asset_mapping)
        if Flags.INFIELD_MIGRATE.is_enabled():
            self.command("infield-configs")(self.infield_configs)
            # Uncomment when the infield data migration is ready.
            # self.command("infield-data")(self.infield_data)

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
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = MigrationPrepareCommand(client=client)
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
        auto_fix: Annotated[
            bool,
            typer.Option(
                "--auto-fix",
                "-a",
                help="If set, the command will try to make the data set external ID a valid space identifier,"
                "if it is not already.",
            ),
        ] = False,
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
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            output_dir = Path(
                questionary.path(
                    "Specify output directory for instance space definitions:", default=str(output_dir)
                ).unsafe_ask()
            )
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.create(
                client,
                creator=InstanceSpaceCreator(client, data_set_external_ids=data_set, auto_fix=auto_fix),
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
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            output_dir = Path(
                questionary.path(
                    "Specify output directory for instance space definitions:", default=str(output_dir)
                ).unsafe_ask()
            )
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()
        elif data_set is None or instance_space is None:
            raise typer.BadParameter("Both data_set and instance_space must be provided together.")

        cmd = MigrationCommand(client=client)
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
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="(Only used with mapping-file) If set, no confirmation prompt will be shown before proceeding with the migration.",
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
            auto_yes=auto_yes,
            verbose=verbose,
            kind="Assets",
            resource_type="asset",
            container_id=ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
        )

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[selected],
                data=AssetCentricMigrationIO(client),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="assets",
            )
        )

    @staticmethod
    def _prepare_asset_centric_arguments(
        client: ToolkitClient,
        mapping_file: Path | None,
        data_set_id: str | None,
        consumption_view: str | None,
        ingestion_mapping: str | None,
        auto_yes: bool,
        dry_run: bool,
        verbose: bool,
        kind: AssetCentricKind,
        resource_type: str,
        container_id: ContainerId,
        event_type: str | None = None,
        event_subtype: str | None = None,
    ) -> tuple[AssetCentricMigrationSelector, bool, bool]:
        if data_set_id is not None and mapping_file is not None:
            raise typer.BadParameter("Cannot specify both data_set_id and mapping_file")
        elif mapping_file is not None:
            file_selector = MigrationCSVFileSelector(datafile=mapping_file, kind=kind)
            selected: AssetCentricMigrationSelector = file_selector

            panel = file_selector.items.print_status()
            if panel is not None:
                client.console.print(panel)
                if not auto_yes:
                    proceed = questionary.confirm(
                        "Do you want to proceed with the migration?", default=False
                    ).unsafe_ask()
                    if not proceed:
                        client.console.print("Migration aborted by user.")
                        raise typer.Abort()
        elif data_set_id is not None:
            parsed_view = parse_view_str(consumption_view) if consumption_view is not None else None
            selected = MigrateDataSetSelector(
                data_set_external_id=data_set_id,
                kind=kind,
                ingestion_mapping=ingestion_mapping,
                preferred_consumer_view=parsed_view,
                event_type=event_type,
                event_subtype=event_subtype,
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
                    filter=ViewSelectFilter(
                        include_global=True,
                        mapped_container=container_id,
                        instance_type="node",
                    ),
                )
                .as_id()
            )
            selected = MigrateDataSetSelector(
                data_set_external_id=selected_data_set_id,
                kind=kind,
                ingestion_mapping=asset_mapping.external_id,
                preferred_consumer_view=preferred_consumer_view,
                event_type=event_type,
                event_subtype=event_subtype,
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()

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
        target: Annotated[
            str,
            typer.Option(
                "--target",
                "-t",
                help="Use 'instances' (default) to migrate events to CogniteActivity"
                " or use 'records' to migrate events to records.",
            ),
        ] = "instances",
        stream_id: Annotated[
            str | None,
            typer.Option(
                "--stream-id",
                help="The external ID of the target stream. Only applicable and required when --target is 'records'.",
            ),
        ] = None,
        event_type: Annotated[
            str | None,
            typer.Option(
                "--type",
                help="Filter events by type. Only events matching this type will be migrated.",
            ),
        ] = None,
        event_subtype: Annotated[
            str | None,
            typer.Option(
                "--subtype",
                help="Filter events by subtype. Only events matching this subtype will be migrated.",
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
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="(Only used with mapping-file) If set, no confirmation prompt will be shown before proceeding with the migration.",
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
        """Migrate Events to CogniteActivity (instances) or to records."""
        if target not in ("instances", "records"):
            raise typer.BadParameter(f"Invalid target '{target}'. Must be 'instances' or 'records'.")
        if target == "records" and stream_id is None:
            raise typer.BadParameter("--stream-id is required when --target is 'records'.")
        if target == "records" and ingestion_mapping is None:
            raise typer.BadParameter("--ingestion-mapping is required when --target is 'records'.")

        client = EnvironmentVariables.create_from_environment().get_client()
        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            auto_yes=auto_yes,
            verbose=verbose,
            kind="Events",
            resource_type="event",
            container_id=ContainerId(space="cdf_cdm", external_id="CogniteActivity"),
            event_type=event_type,
            event_subtype=event_subtype,
        )

        cmd = MigrationCommand(client=client)

        if target == "records":
            assert stream_id is not None
            cmd.validate_stream_exists(client, stream_id)
            cmd.run(
                lambda: cmd.migrate(
                    selectors=[selected],
                    data=RecordsMigrationIO(client, stream_external_id=stream_id),
                    mapper=RecordsMapper(client),
                    log_dir=log_dir,
                    dry_run=dry_run,
                    verbose=verbose,
                    user_log_filestem="events-records",
                )
            )
        else:
            cmd.run(
                lambda: cmd.migrate(
                    selectors=[selected],
                    data=AssetCentricMigrationIO(client),
                    mapper=AssetCentricMapper(client),
                    log_dir=log_dir,
                    dry_run=dry_run,
                    verbose=verbose,
                    user_log_filestem="events",
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
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="(Only used with mapping-file) If set, no confirmation prompt will be shown before proceeding with the migration.",
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
        client = EnvironmentVariables.create_from_environment().get_client()

        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            auto_yes=auto_yes,
            verbose=verbose,
            kind="TimeSeries",
            resource_type="timeseries",
            container_id=ContainerId(space="cdf_cdm", external_id="CogniteTimeSeries"),
        )
        if data_set_id is None and mapping_file is None:
            skip_linking = not questionary.confirm(
                "Do you want to link old and new TimeSeries?", default=not skip_linking
            ).unsafe_ask()

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[selected],
                data=AssetCentricMigrationIO(client, skip_linking=skip_linking),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="timeseries",
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
        auto_yes: Annotated[
            bool,
            typer.Option(
                "--yes",
                "-y",
                help="(Only used with mapping-file) If set, no confirmation prompt will be shown before proceeding with the migration.",
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
        client = EnvironmentVariables.create_from_environment().get_client()

        selected, dry_run, verbose = cls._prepare_asset_centric_arguments(
            client=client,
            mapping_file=mapping_file,
            data_set_id=data_set_id,
            consumption_view=consumption_view,
            ingestion_mapping=ingestion_mapping,
            dry_run=dry_run,
            auto_yes=auto_yes,
            verbose=verbose,
            kind="FileMetadata",
            resource_type="file",
            container_id=ContainerId(space="cdf_cdm", external_id="CogniteFile"),
        )
        cmd = MigrationCommand(client=client)

        if data_set_id is None and mapping_file is None:
            skip_linking = not questionary.confirm(
                "Do you want to link old and new Files?", default=not skip_linking
            ).unsafe_ask()

        cmd.run(
            lambda: cmd.migrate(
                selectors=[selected],
                data=AssetCentricMigrationIO(client, skip_linking=skip_linking),
                mapper=AssetCentricMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="files",
            )
        )

    @classmethod
    def annotations(
        cls,
        ctx: typer.Context,
        mapping_file: Annotated[
            Path | None,
            typer.Option(
                "--mapping-file",
                "-m",
                help="Path to the mapping file that contains the mapping from Annotations to CogniteDiagramAnnotation. "
                "This file is expected to have the following columns: [id, space, externalId, ingestionView].",
            ),
        ] = None,
        data_set_id: Annotated[
            str | None,
            typer.Option(
                "--data-set-id",
                "-s",
                help="The data set ID to select for the annotations to migrate. If not provided and the mapping file is not provided, "
                "an interactive selection will be performed to select the data set to migrate annotations from.",
            ),
        ] = None,
        instance_space: Annotated[
            str | None,
            typer.Option(
                "--instance-space",
                "-i",
                help="The instance space to use for the migrated annotations. Required when using --data-set-id.",
            ),
        ] = None,
        asset_annotation_mapping: Annotated[
            str | None,
            typer.Option(
                "--asset-annotation-mapping",
                "-a",
                help="The ingestion mapping to use for asset-linked annotations. If not provided, "
                "the default mapping (cdf_asset_annotations_mapping) will be used.",
            ),
        ] = None,
        file_annotation_mapping: Annotated[
            str | None,
            typer.Option(
                "--file-annotation-mapping",
                "-f",
                help="The ingestion mapping to use for file-linked annotations. If not provided, "
                "the default mapping (cdf_file_annotations_mapping) will be used.",
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
        """Migrate Annotations to CogniteDiagramAnnotation edges in data modeling.

        Annotations are diagram annotations that link assets or files to other resources. This command
        migrates them to edges in the data modeling space, preserving the relationships and metadata.
        """
        client = EnvironmentVariables.create_from_environment().get_client()

        if data_set_id is not None and mapping_file is not None:
            raise typer.BadParameter("Cannot specify both data_set_id and mapping_file")
        elif mapping_file is not None:
            selected: AssetCentricMigrationSelector = MigrationCSVFileSelector(
                datafile=mapping_file, kind="Annotations"
            )
            annotation_io = AnnotationMigrationIO(client)
        elif data_set_id is not None:
            if instance_space is None:
                raise typer.BadParameter("--instance-space is required when using --data-set-id")
            selected = MigrateDataSetSelector(data_set_external_id=data_set_id, kind="Annotations")
            annotation_io = AnnotationMigrationIO(
                client,
                instance_space=instance_space,
                default_asset_annotation_mapping=asset_annotation_mapping,
                default_file_annotation_mapping=file_annotation_mapping,
            )
        else:
            # Interactive selection
            selector = FileMetadataInteractiveSelect(client, "migrate")
            selected_data_set_id = selector.select_data_set(allow_empty=False)
            dm_selector = DataModelingSelect(client, "migrate")
            selected_instance_space = dm_selector.select_instance_space(
                multiselect=False,
                message="In which instance space do you want to create the annotations?",
                include_empty=True,
            )
            if selected_instance_space is None:
                raise typer.Abort()
            asset_annotations_selector = ResourceViewMappingInteractiveSelect(client, "migrate asset annotations")
            asset_annotation_mapping = asset_annotations_selector.select_resource_view_mapping(
                resource_type="assetAnnotation",
            ).external_id
            file_annotations_selector = ResourceViewMappingInteractiveSelect(client, "migrate file annotations")
            file_annotation_mapping = file_annotations_selector.select_resource_view_mapping(
                resource_type="fileAnnotation",
            ).external_id

            selected = MigrateDataSetSelector(data_set_external_id=selected_data_set_id, kind="Annotations")
            annotation_io = AnnotationMigrationIO(
                client,
                instance_space=selected_instance_space,
                default_asset_annotation_mapping=asset_annotation_mapping,
                default_file_annotation_mapping=file_annotation_mapping,
            )

            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[selected],
                data=annotation_io,
                mapper=AssetCentricMapper[AnnotationResponse](client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="annotations",
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
        allow_missing_ref: Annotated[
            bool,
            typer.Option(
                "--allow-missing-ref",
                "-a",
                help="If set, the command will migrate Canvases that reference resources that have not been migrated to data modeling. "
                "If not set, the migration will fail if any referenced resource are missing.",
            ),
        ] = False,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where migration logs will be stored.",
            ),
        ] = Path(f"migration_logs_{TODAY}"),
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
        if external_id is None:
            interactive = InteractiveCanvasSelect(client)
            external_id = interactive.select_external_ids()
            log_dir = Path(
                questionary.path("Specify log directory for migration logs:", default=str(log_dir)).unsafe_ask()
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()

        cmd = MigrationCommand(client=client)
        selector = CanvasExternalIdSelector(external_ids=tuple(external_id))
        cmd.run(
            lambda: cmd.migrate(
                selectors=[selector],
                # Migration should not modify solution tags - there are used across multiple applications
                # (Canvas, InField)
                data=CanvasIO(client, exclude_existing_version=True, include_solution_tags=False),
                mapper=CanvasMapper(client, dry_run=dry_run, skip_on_missing_ref=not allow_missing_ref),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="canvas",
            )
        )

    @staticmethod
    def charts(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID of the Chart to migrate. If not provided, an interactive selection will be "
                "performed to select the Charts to migrate."
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where migration logs will be stored.",
            ),
        ] = Path(f"migration_logs_{TODAY}"),
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the migration will not be executed, but only a report of "
                "what would be done is printed. This is useful for checking that all time series referenced by the Charts "
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
        """Migrate Charts from time series references to data modeling in CDF.

        This command expects that the CogniteMigration data model is already deployed, and that the Mapping view
        is populated with the mapping from time series to the new data modeling resources.
        """
        client = EnvironmentVariables.create_from_environment().get_client()

        selected_external_ids: list[str]
        if external_id:
            selected_external_ids = external_id
        else:
            selected_external_ids = InteractiveChartSelect(client).select_external_ids()

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[ChartExternalIdSelector(external_ids=tuple(selected_external_ids))],
                data=ChartIO(client),
                mapper=ChartMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="charts",
            )
        )

    @staticmethod
    def three_d(
        ctx: typer.Context,
        id: Annotated[
            list[int] | None,
            typer.Argument(
                help="The ID of the 3D Model to migrate. If not provided, an interactive selection will be "
                "performed to select the 3D Models to migrate."
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where migration logs will be stored.",
            ),
        ] = Path(f"migration_logs_{TODAY}"),
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the migration will not be executed, but only a report of "
                "what would be done is printed. This is useful for checking that all resources referenced by the 3D Models "
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
        """Migrate 3D Models from Asset-Centric to data modeling in CDF.

        This command expects that the CogniteMigration data model is already deployed, and that the Mapping view
        is populated with the mapping from Asset-Centric resources to the new data modeling resources.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        selected_ids: list[int]
        if id:
            selected_ids = id
        else:
            selected_models = ThreeDInteractiveSelect(client, "migrate").select_three_d_models("classic")
            selected_ids = [model.id for model in selected_models]

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[ThreeDModelIdSelector(ids=tuple(selected_ids))],
                data=ThreeDMigrationIO(client),
                mapper=ThreeDMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="three_d_models",
            )
        )

    @staticmethod
    def three_d_asset_mapping(
        ctx: typer.Context,
        model_id: Annotated[
            list[int] | None,
            typer.Argument(
                help="The IDs of the 3D model to migrate asset mappings for. If not provided, an interactive selection will be "
                "performed to select the."
            ),
        ] = None,
        object_3D_space: Annotated[
            str | None,
            typer.Option(
                "--object-3d-space",
                "-o",
                help="The instance space to ceate the 3D object nodes in.",
            ),
        ] = None,
        cad_node_space: Annotated[
            str | None,
            typer.Option(
                "--cad-node-space",
                "-c",
                help="The instance space to create the CAD node nodes in.",
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where migration logs will be stored.",
            ),
        ] = Path(f"migration_logs_{TODAY}"),
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
        """Migrate 3D Model Asset Mappings from Asset-Centric to data modeling in CDF.

        This command expects that the selected 3D model has already been migrated to data modeling.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        selected_ids: list[int]
        if model_id is not None:
            selected_ids = model_id
        else:
            # Interactive selection
            selected_models = ThreeDInteractiveSelect(client, "migrate").select_three_d_models("dm")
            selected_ids = [model.id for model in selected_models]
            space_selector = DataModelingSelect(client, "migrate")
            object_3D_space = space_selector.select_instance_space(
                multiselect=False,
                message="In which instance space do you want to create the 3D Object nodes?",
                include_empty=True,
            )
            cad_node_space = space_selector.select_instance_space(
                multiselect=False,
                message="In which instance space do you want to create the CAD Node nodes?",
                include_empty=True,
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()

        if object_3D_space is None or cad_node_space is None:
            raise typer.BadParameter(
                "--object-3d-space and --cad-node-space are required when specifying IDs directly."
            )

        cmd = MigrationCommand(client=client)
        cmd.run(
            lambda: cmd.migrate(
                selectors=[ThreeDModelIdSelector(ids=tuple(selected_ids))],
                data=ThreeDAssetMappingMigrationIO(
                    client, object_3D_space=object_3D_space, cad_node_space=cad_node_space
                ),
                mapper=ThreeDAssetMapper(client),
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="three_d_asset_mapping",
            )
        )

    @staticmethod
    def infield_configs(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external IDs of the APM Configurations to create Infield V2 configurations for. "
                "If not provided, an interactive selection will be performed to select the APM Configurations."
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Path to the directory where the Infield V2 configuration definitions will be dumped. It is recommended "
                "to govern these configurations in a git repository.",
            ),
        ] = Path("tmp"),
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

        cmd = MigrationCommand(client=client)
        if external_id is None:
            apm_configs = APMConfigInteractiveSelect(client, "migrate").select_apm_configs()
            output_dir = Path(
                questionary.path(
                    "Specify output directory for Infield V2 configuration definitions:", default=str(output_dir)
                ).unsafe_ask()
            )
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()
        else:
            apm_configs = None

        cmd.run(
            lambda: cmd.create(
                client,
                creator=InfieldV2ConfigCreator(client, external_id, apm_configs),
                output_dir=output_dir,
                dry_run=False,
                deploy=False,
                verbose=verbose,
            )
        )

    @staticmethod
    def infield_data(
        ctx: typer.Context,
        source_space: Annotated[
            str | None,
            typer.Option(
                "--source-space",
                "-s",
                help="The instance space to select Infield data from. If not provided, an interactive selection will be performed to select the instance space.",
            ),
        ] = None,
        target_space: Annotated[
            str | None,
            typer.Option(
                "--target-space",
                "-t",
                help="The instance space to migrate Infield data to. If not provided, an interactive selection will be performed to select the target instance space.",
            ),
        ] = None,
        log_dir: Annotated[
            Path,
            typer.Option(
                "--log-dir",
                "-l",
                help="Path to the directory where migration logs will be stored.",
            ),
        ] = Path(f"migration_logs_{TODAY}"),
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
        """Migrates Infield data from existing APM instance spaces in CDF to the new InfieldOnCDM data model."""
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = MigrationCommand(client=client)
        apm_configs = client.infield.apm_config.list(limit=None)
        source_candidates = {
            location.app_data_instance_space
            for config in apm_configs
            if config.feature_configuration
            for location in config.feature_configuration.root_location_configurations or []
            if location.app_data_instance_space
        }
        infield_cdm_configs = client.infield.cdm_config.list(limit=None)
        target_candidates = {
            config.data_storage.app_instance_space
            for config in infield_cdm_configs
            if config.data_storage and config.data_storage.app_instance_space
        }
        if not source_candidates:
            raise typer.BadParameter("No APM Configurations with app data space found. Cannot migrate Infield data.")
        if not target_candidates:
            raise typer.BadParameter(
                "No InfieldOnCDM Configurations with app instance space found. Cannot migrate Infield data."
            )
        if source_space is None and target_space is None:
            source_stats = client.data_modeling.statistics.spaces.retrieve(list(source_candidates))
            if not source_stats:
                raise typer.BadParameter(
                    f"Source spaces {humanize_collection(source_candidates)} do not exist or cannot be accessed. Please ensure the APM instance space contains data and can be accessed."
                )
            source_space = questionary.select(
                "Select the instance space to migrate Infield data from:",
                choices=[
                    questionary.Choice(
                        title=f"{item.space} (contains {item.nodes:,} nodes and {item.edges:,} edges)",
                        value=item.space,
                    )
                    for item in source_stats
                ],
            ).unsafe_ask()
            target_stats = client.data_modeling.statistics.spaces.retrieve(list(target_candidates))
            if not target_stats:
                raise typer.BadParameter(
                    f"Target spaces {humanize_collection(target_candidates)} do not exist or cannot be accessed. Please create the instance space or ensure you can access it."
                )
            target_space = questionary.select(
                "Select the instance space to migrate Infield data to:",
                choices=[
                    questionary.Choice(
                        title=f"{item.space} (contains {item.nodes:,} nodes and {item.edges:,} edges)",
                        value=item.space,
                    )
                    for item in target_stats
                ],
            ).unsafe_ask()
            log_dir = Path(
                questionary.path("Specify log directory for migration logs:", default=str(log_dir)).unsafe_ask()
            )
            dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
            verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()
        elif source_space is not None and target_space is not None:
            errors: list[str] = []
            if source_space not in source_candidates:
                errors.append(
                    f"Source space '{source_space}' is not a valid source for Infield data migration. Available source spaces are: {humanize_collection(source_candidates)}."
                )
            if target_space not in target_candidates:
                errors.append(
                    f"Target space '{target_space}' is not a valid target for Infield data migration. Available target spaces are: {humanize_collection(target_candidates)}."
                )
            if errors:
                raise typer.BadParameter("\n".join(errors))
        else:
            raise typer.BadParameter("Either both --source-space and --target-space must be provided, or neither.")

        space_mapping = {
            source_space: target_space,
            # The users are stored in this space are unchanged. This ensures that all direct relations
            # to users are preserved.
            "cognite_app_data": "cognite_app_data",
        }
        infield_mappings = create_infield_data_mappings()
        schedule_selector = create_infield_schedule_selector()
        selectors: list[InstanceViewSelector | InstanceQuerySelector] = []
        schedule_mapping: ViewToViewMapping | None = None
        for mapping in infield_mappings:
            if mapping.source_view.external_id == "Schedule":
                # Special case for schedules, see create_infield_schedule_query for docs on why.
                selectors.append(schedule_selector)
                schedule_mapping = mapping
            else:
                selectors.append(
                    InstanceViewSelector(
                        view=SelectedView(
                            space=mapping.source_view.space,
                            external_id=mapping.source_view.external_id,
                            version=mapping.source_view.version,
                        ),
                        instance_spaces=(source_space,),
                        edge_types=tuple(mapping.edge_mapping.keys()) if mapping.edge_mapping else None,
                    )
                )
        if schedule_mapping is None:
            raise ValueError("No mapping for Schedule view found in infield_data_mappings.yaml")
        connection_creator = ConnectionCreator(
            client, space_mapping=space_mapping, custom_mappings=[InFieldAssetMapping(client)]
        )
        mapper = FDMtoCDMMapper(
            client,
            infield_mappings,
            connection_creator=connection_creator,
            custom_properties_mappings=[InFieldConditionMapping(infield_mappings)],
            custom_instance_mappings={
                InFieldLegacyToCDMScheduleMapper.SCHEDULE_VIEW: InFieldLegacyToCDMScheduleMapper(
                    client, connection_creator, schedule_mapping
                )
            },
        )
        cmd.run(
            lambda: cmd.migrate(
                selectors=selectors,
                data=InstanceIO(client),
                mapper=mapper,
                log_dir=log_dir,
                dry_run=dry_run,
                verbose=verbose,
                user_log_filestem="infield_data",
            )
        )
