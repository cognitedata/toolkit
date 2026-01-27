from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.client.resource_classes.legacy.raw import RawTable
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.constants import DATA_DEFAULT_DIR
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.storageio import (
    AssetIO,
    CanvasIO,
    ChartIO,
    DatapointsIO,
    DataSelector,
    EventIO,
    FileContentIO,
    FileMetadataIO,
    HierarchyIO,
    InstanceIO,
    RawIO,
    StorageIO,
    TimeSeriesIO,
)
from cognite_toolkit._cdf_tk.storageio.selectors import (
    AssetSubtreeSelector,
    CanvasExternalIdSelector,
    CanvasSelector,
    ChartExternalIdSelector,
    ChartSelector,
    DataPointsDataSetSelector,
    DataSetSelector,
    FileIdentifierSelector,
    InstanceSpaceSelector,
    RawTableSelector,
    SelectedTable,
    SelectedView,
)
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import FileInternalID
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetCentricInteractiveSelect,
    AssetInteractiveSelect,
    DataModelingSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    InteractiveCanvasSelect,
    InteractiveChartSelect,
    RawTableInteractiveSelect,
    TimeSeriesInteractiveSelect,
)


class RawFormats(str, Enum):
    ndjson = "ndjson"
    yaml = "yaml"


class AssetCentricFormats(str, Enum):
    csv = "csv"
    parquet = "parquet"
    ndjson = "ndjson"


class FileContentFormats(str, Enum):
    ndjson = "ndjson"


class HierarchyFormats(str, Enum):
    ndjson = "ndjson"


class DatapointFormats(str, Enum):
    csv = "csv"
    parquet = "parquet"


class DatapointsDataTypes(str, Enum):
    numeric = "numeric"
    string = "string"


class InstanceFormats(str, Enum):
    ndjson = "ndjson"


class ChartFormats(str, Enum):
    ndjson = "ndjson"


class CanvasFormats(str, Enum):
    ndjson = "ndjson"


class InstanceTypes(str, Enum):
    node = "node"
    edge = "edge"


class CompressionFormat(str, Enum):
    gzip = "gzip"
    none = "none"


DEFAULT_DOWNLOAD_DIR = Path(DATA_DEFAULT_DIR)


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.download_main)
        self.command("raw")(self.download_raw_cmd)
        self.command("assets")(self.download_assets_cmd)
        self.command("timeseries")(self.download_timeseries_cmd)
        self.command("events")(self.download_events_cmd)
        self.command("files")(self.download_files_cmd)
        self.command("hierarchy")(self.download_hierarchy_cmd)
        if Flags.EXTEND_DOWNLOAD.is_enabled():
            self.command("datapoints")(self.download_datapoints_cmd)
        self.command("instances")(self.download_instances_cmd)
        self.command("charts")(self.download_charts_cmd)
        self.command("canvas")(self.download_canvas_cmd)

    @staticmethod
    def download_main(ctx: typer.Context) -> None:
        """Commands to download data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf download --help[/] for more information.")
        return None

    @staticmethod
    def download_raw_cmd(
        ctx: typer.Context,
        tables: Annotated[
            list[str] | None,
            typer.Argument(
                help="List of tables to download. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        database: Annotated[
            str | None,
            typer.Option(
                "--database",
                "-d",
                help="Database to download from. If not provided, the user will be prompted to select a database.",
            ),
        ] = None,
        file_format: Annotated[
            RawFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the raw tables in. Supported formats: ndjson, yaml",
            ),
        ] = RawFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the raw tables. Supported formats: gzip, none.",
            ),
        ] = CompressionFormat.gzip,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the raw tables.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of records to download from each table. Use -1 to download all records.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download RAW tables from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)

        if tables and database:
            selectors = [RawTable(db_name=database, table_name=table) for table in tables]
        elif tables and not database:
            raise typer.BadParameter(
                "The '--database' option is required when specifying tables as arguments.",
                param_hint="--database",
            )
        elif not tables and database:
            selectors = RawTableInteractiveSelect(client, "download").select_tables(database=database)
        else:
            selectors = RawTableInteractiveSelect(client, "download").select_tables()

        cmd.run(
            lambda: cmd.download(
                selectors=[
                    RawTableSelector(table=SelectedTable(db_name=item.db_name, table_name=item.table_name))
                    for item in selectors
                ],
                io=RawIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    def download_assets_cmd(
        self,
        ctx: typer.Context,
        data_sets: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="List of data sets to download assets from. If this is not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            AssetCentricFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the assets in.",
            ),
        ] = AssetCentricFormats.csv,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the assets.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the assets.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of assets to download from each dataset/hierarchy. Use -1 to download all assets.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download assets from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_sets is None:
            data_sets, file_format, compression, output_dir, limit = self._asset_centric_interactive(
                AssetInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "assets",
            )

        selectors = [DataSetSelector(kind="Assets", data_set_external_id=data_set) for data_set in data_sets]
        cmd = DownloadCommand(client=client)
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=AssetIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @classmethod
    def _asset_centric_interactive(
        cls,
        selector: AssetCentricInteractiveSelect,
        file_format: AssetCentricFormats,
        compression: CompressionFormat,
        output_dir: Path,
        limit: int,
        display_name: str,
        max_limit: int | None = None,
        available_formats: type[Enum] = AssetCentricFormats,
    ) -> tuple[list[str], AssetCentricFormats, CompressionFormat, Path, int]:
        data_sets = selector.select_data_sets()
        file_format = questionary.select(
            f"Select format to download the {display_name} in:",
            choices=[Choice(title=format_.value, value=format_) for format_ in available_formats],
            default=file_format,
        ).unsafe_ask()
        compression = questionary.select(
            f"Select compression format to use when downloading the {display_name}:",
            choices=[Choice(title=comp.value, value=comp) for comp in CompressionFormat],
            default=compression,
        ).unsafe_ask()
        output_dir = Path(
            questionary.path(
                f"Where to download the {display_name}:",
                default=str(output_dir),
                only_directories=True,
            ).unsafe_ask()
        )
        limit = int(
            questionary.text(
                f"The maximum number of {display_name} to download from each dataset. Use -1 to download all {display_name}.",
                default=str(limit),
                validate=lambda value: value.lstrip("-").isdigit() and (max_limit is None or int(value) <= max_limit),
            ).unsafe_ask()
        )
        return data_sets, file_format, compression, output_dir, limit

    def download_timeseries_cmd(
        self,
        ctx: typer.Context,
        data_sets: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="List of data sets to download time series from. If this is not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            AssetCentricFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the time series in.",
            ),
        ] = AssetCentricFormats.csv,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the time series.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the time series.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of time series to download from each dataset. Use -1 to download all time series.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download time series from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_sets is None:
            data_sets, file_format, compression, output_dir, limit = self._asset_centric_interactive(
                TimeSeriesInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "time series",
            )

        selectors = [DataSetSelector(kind="TimeSeries", data_set_external_id=data_set) for data_set in data_sets]
        cmd = DownloadCommand(client=client)
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=TimeSeriesIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    def download_events_cmd(
        self,
        ctx: typer.Context,
        data_sets: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="List of data sets to download events from. If this is not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            AssetCentricFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the events in.",
            ),
        ] = AssetCentricFormats.csv,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the events.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the events.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of events to download from each dataset. Use -1 to download all events.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download events from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_sets is None:
            data_sets, file_format, compression, output_dir, limit = self._asset_centric_interactive(
                EventInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "events",
            )

        selectors = [DataSetSelector(kind="Events", data_set_external_id=data_set) for data_set in data_sets]
        cmd = DownloadCommand(client=client)

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=EventIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    def download_files_cmd(
        self,
        ctx: typer.Context,
        data_sets: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="List of data sets to download file metadata from. If this is not provided, an interactive selection will be made.",
            ),
        ] = None,
        include_file_contents: Annotated[
            bool,
            typer.Option(
                "--include-file-contents",
                "-c",
                help="Whether to include file contents when downloading assets. Note if you enable this option, you can"
                "only download 1000 files at a time.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = False,
        file_format: Annotated[
            AssetCentricFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the file metadata in.",
            ),
        ] = AssetCentricFormats.csv,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the file metadata.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the file metadata.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of file metadata to download from each dataset. Use -1 to download all file metadata.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download file metadata from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if data_sets is None:
            if Flags.EXTEND_DOWNLOAD.is_enabled():
                include_file_contents = questionary.select(
                    "Do you want to include file contents when downloading file metadata?",
                    choices=[
                        Choice(title="Yes", value=True),
                        Choice(title="No", value=False),
                    ],
                ).unsafe_ask()
            else:
                include_file_contents = False

            available_formats = FileContentFormats if include_file_contents else AssetCentricFormats
            file_format = FileContentFormats.ndjson if include_file_contents else file_format  # type: ignore[assignment]
            data_sets, file_format, compression, output_dir, limit = self._asset_centric_interactive(
                FileMetadataInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit if not include_file_contents else 1000,
                "file metadata",
                max_limit=1000 if include_file_contents else None,
                available_formats=available_formats,
            )

        io: StorageIO
        selectors: list[DataSelector]
        if include_file_contents:
            if limit == -1 or limit > 1000:
                limit = 1000
                print(
                    "[yellow]When including file contents, the maximum number of files that can be downloaded at a time is 1000. "
                )
            if file_format == AssetCentricFormats.csv or file_format == AssetCentricFormats.parquet:
                print(
                    "[red]When including file contents, the only supported format is ndjson. Overriding the format to ndjson.[/]"
                )
                file_format = AssetCentricFormats.ndjson
            files = client.files.list(data_set_external_ids=data_sets, limit=limit)
            selector = FileIdentifierSelector(
                identifiers=tuple([FileInternalID(internal_id=file.id) for file in files])
            )
            selectors = [selector]
            io = FileContentIO(client, output_dir / sanitize_filename(selector.group))
        else:
            selectors = [DataSetSelector(kind="FileMetadata", data_set_external_id=data_set) for data_set in data_sets]
            io = FileMetadataIO(client)

        cmd = DownloadCommand(client=client)
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,  # type: ignore[misc]
                io=io,
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_hierarchy_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            str | None,
            typer.Argument(
                help="The asset hierarchy to download.",
            ),
        ] = None,
        file_format: Annotated[
            HierarchyFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format for downloading the asset hierarchy.",
            ),
        ] = HierarchyFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the assets.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the asset hierarchy.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of resources to download for each type. Use -1 to download all assets.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download an asset hierarchy from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)

        if hierarchy is None:
            selector = AssetInteractiveSelect(client, "download")
            hierarchy = selector.select_hierarchy(allow_empty=False)

        selectors = [
            # MyPy cannot see that resource_type is one of the allowed literals.
            AssetSubtreeSelector(hierarchy=hierarchy, kind=resource_type)  # type: ignore[arg-type]
            for resource_type in ["Assets", "Events", "FileMetadata", "TimeSeries"]
        ]
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=HierarchyIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_instances_cmd(
        ctx: typer.Context,
        instance_space: Annotated[
            str | None,
            typer.Option(
                "--instance-space",
                "-s",
                help="The instance space to download instances from. If not provided, an interactive "
                "selection will be made.",
            ),
        ] = None,
        schema_space: Annotated[
            str | None,
            typer.Option(
                "--schema-space",
                "-c",
                help="The schema space where the views are located.",
            ),
        ] = None,
        view_external_ids: Annotated[
            list[str] | None,
            typer.Option(
                "--view",
                "-w",
                help="List of view external IDs to download properties for the "
                "instances. To specify version use a forward slash, e.g. viewExternalId/v1.",
            ),
        ] = None,
        instance_type: Annotated[
            InstanceTypes,
            typer.Option(
                "--instance-type",
                "-t",
                help="The type of instances to download.",
            ),
        ] = InstanceTypes.node,
        file_format: Annotated[
            InstanceFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the instances in.",
            ),
        ] = InstanceFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the instances.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the instances.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of instances to download from each view. Use -1 to download all.",
            ),
        ] = 10_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download Instances from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)

        client = EnvironmentVariables.create_from_environment().get_client()
        if instance_space is None:
            selector = DataModelingSelect(client, "download instances")
            selected_instance_space = selector.select_instance_space(multiselect=False)
            selected_instance_type = selector.select_instance_type()
            selected_schema_space = selector.select_schema_space(
                include_global=True, message="In which space is the views with instance properties located?"
            ).space
            selected_views = selector.select_view(
                multiselect=True,
                space=selected_schema_space,
                message="Select views to download instance properties from.",
                include_global=True,
                instance_type=selected_instance_type,
            )
            selectors: list[InstanceSpaceSelector] = [
                InstanceSpaceSelector(
                    instance_space=selected_instance_space,
                    view=SelectedView(
                        space=selected_schema_space,
                        external_id=view.external_id,
                        version=view.version,
                    ),
                    instance_type=selected_instance_type,
                )
                for view in selected_views
            ]
        elif schema_space is None and view_external_ids is None:
            selectors = [InstanceSpaceSelector(instance_space=instance_space, instance_type=instance_type.value)]
        elif schema_space is not None and view_external_ids is not None:
            selectors = [
                InstanceSpaceSelector(
                    instance_space=instance_space,
                    view=SelectedView(
                        space=schema_space,
                        external_id=view_id_str.split("/", maxsplit=1)[0],
                        version=view_id_str.split("/", maxsplit=1)[1] if "/" in view_id_str else None,
                    ),
                    instance_type=instance_type.value,
                )
                for view_id_str in view_external_ids
            ]
        else:
            raise typer.BadParameter(
                "Both '--schema-space' and '--view' must be provided together.",
                param_hint="--view",
            )

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=InstanceIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_datapoints_cmd(
        dataset: Annotated[
            str | None,
            typer.Argument(
                help="The dataset to download timeseries from. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        start_time: Annotated[
            str | None,
            typer.Option(
                "--start-time",
                "-s",
                help="The start time for the datapoints to download. Can be in RFC3339 format or as a relative time (e.g., '1d-ago'). If not provided, all datapoints from the beginning will be downloaded.",
            ),
        ] = None,
        end_time: Annotated[
            str | None,
            typer.Option(
                "--end-time",
                "-e",
                help="The end time for the datapoints to download. Can be in RFC3339 format or as a relative time (e.g., '1d-ago'). If not provided, all datapoints up to the latest will be downloaded.",
            ),
        ] = None,
        datapoint_type: Annotated[
            DatapointsDataTypes,
            typer.Option(
                "--data-type",
                "-d",
                help="The type of datapoints to download.",
            ),
        ] = DatapointsDataTypes.numeric,
        file_format: Annotated[
            DatapointFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format for downloading the datapoints.",
            ),
        ] = DatapointFormats.csv,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the datapoints.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of timeseries to download datapoints from. Use -1 to download all timeseries."
                "The maximum number of datapoints in total is 10 million and 100 000 per timeseries.",
                max=10_000_000,
            ),
        ] = 1000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download Datapoints from CDF into a temporary ."""
        client = EnvironmentVariables.create_from_environment().get_client()
        if dataset is None:
            interactive = TimeSeriesInteractiveSelect(client, "download datapoints")
            dataset = interactive.select_data_set(allow_empty=False)

            datapoint_type = questionary.select(
                "Select the type of datapoints to download:",
                choices=[Choice(title=dt.value, value=dt) for dt in DatapointsDataTypes],
                default=datapoint_type,
            ).unsafe_ask()

            start_time = (
                questionary.text(
                    "Enter the start time for the datapoints to download (RFC3339 format or relative time, e.g., '1d-ago'). Leave empty to download from the beginning.",
                    default=start_time or "",
                ).unsafe_ask()
                or None
            )
            end_time = (
                questionary.text(
                    "Enter the end time for the datapoints to download (RFC3339 format or relative time, e.g., '1d-ago'). Leave empty to download up to the latest.",
                    default=end_time or "",
                ).unsafe_ask()
                or None
            )
            file_format = questionary.select(
                "Select format to download the datapoints in:",
                choices=[Choice(title=format_.value, value=format_) for format_ in DatapointFormats],
                default=file_format,
            ).unsafe_ask()
            output_dir = Path(
                questionary.path(
                    "Where to download the datapoints:", default=str(output_dir), only_directories=True
                ).unsafe_ask()
            )
            limit = int(
                questionary.text(
                    "The maximum number of timeseries to download datapoints from. Use -1 to download all timeseries."
                    "The maximum number of datapoints in total is 10 million and 100 000 per timeseries.",
                    default=str(limit),
                    validate=lambda value: value.lstrip("-").isdigit() and (int(value) == -1 or int(value) > 0),
                ).unsafe_ask()
            )

        cmd = DownloadCommand(client=client)
        selector = DataPointsDataSetSelector(
            data_set_external_id=dataset,
            start=start_time,
            end=end_time,
            data_type=datapoint_type.value,
        )
        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=DatapointsIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression="none",
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_charts_cmd(
        ctx: typer.Context,
        external_ids: Annotated[
            list[str] | None,
            typer.Argument(
                help="List of chart external IDs to download. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            ChartFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format for downloading the charts.",
            ),
        ] = ChartFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the instances.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the charts.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of charts to download. Use -1 to download all charts.",
            ),
        ] = 1000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download Charts from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)
        selector: ChartSelector
        if external_ids is None:
            selected_external_ids = InteractiveChartSelect(client).select_external_ids()
            selector = ChartExternalIdSelector(external_ids=tuple(selected_external_ids))
        else:
            selector = ChartExternalIdSelector(external_ids=tuple(external_ids))

        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=ChartIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_canvas_cmd(
        ctx: typer.Context,
        external_ids: Annotated[
            list[str] | None,
            typer.Argument(
                help="List of canvas external IDs to download. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            CanvasFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format for downloading the canvas.",
            ),
        ] = CanvasFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the canvas.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the canvas.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of canvas to download. Use -1 to download all canvas.",
            ),
        ] = 1000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download Canvas from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)
        selector: CanvasSelector
        if external_ids is None:
            selected_external_ids = InteractiveCanvasSelect(client).select_external_ids()
            selector = CanvasExternalIdSelector(external_ids=tuple(selected_external_ids))
        else:
            selector = CanvasExternalIdSelector(external_ids=tuple(external_ids))

        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=CanvasIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )
