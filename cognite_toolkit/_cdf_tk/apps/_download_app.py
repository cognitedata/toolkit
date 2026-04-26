from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, RawTableId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import EdgeProperty
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.constants import DATA_DEFAULT_DIR
from cognite_toolkit._cdf_tk.dataio import (
    AssetDataIO,
    CanvasIO,
    ChartIO,
    CogniteFileContentIO,
    DataIO,
    DatapointsIO,
    DataSelector,
    EventDataIO,
    FileMetadataContentIO,
    FileMetadataDataIO,
    HierarchyIO,
    InstanceIO,
    RawIO,
    RecordIO,
    TimeSeriesDataIO,
)
from cognite_toolkit._cdf_tk.dataio.selectors import (
    AssetSubtreeSelector,
    CanvasExternalIdSelector,
    CanvasSelector,
    ChartExternalIdSelector,
    ChartSelector,
    CogniteFileFilesSelectorV2,
    DataPointsDataSetSelector,
    DataSetSelector,
    FileMetadataFilesSelectorV2,
    InstanceSelector,
    InstanceSpaceSelector,
    InstanceViewSelector,
    InternalWithNameId,
    NodeWithNameId,
    RawTableSelector,
    SelectedTable,
    SelectedView,
)
from cognite_toolkit._cdf_tk.dataio.selectors._records import (
    RecordContainerSelector,
    SelectedContainer,
    SelectedStream,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetCentricInteractiveSelect,
    AssetInteractiveSelect,
    DataModelingSelect,
    DocumentsInteractiveSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    InteractiveCanvasSelect,
    InteractiveChartSelect,
    RawTableInteractiveSelect,
    RecordInteractiveSelect,
    TimeSeriesInteractiveSelect,
    ViewSelectFilter,
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


if Flags.EXTEND_DOWNLOAD.is_enabled():

    class InstanceFormats(str, Enum):
        ndjson = "ndjson"
        csv = "csv"
        parquet = "parquet"
else:

    class InstanceFormats(str, Enum):  # type: ignore[no-redef]
        ndjson = "ndjson"


class ChartFormats(str, Enum):
    ndjson = "ndjson"


class CanvasFormats(str, Enum):
    ndjson = "ndjson"


class RecordFormats(str, Enum):
    ndjson = "ndjson"


class InstanceTypes(str, Enum):
    node = "node"
    edge = "edge"


class ApiFormat(str, Enum):
    request = "request"
    response = "response"


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
            self.command("records")(self.download_records_cmd)
        self.command("instances")(self.download_instances_cmd)
        self.command("charts")(self.download_charts_cmd)
        self.command("canvas")(self.download_canvas_cmd)

    @staticmethod
    def download_main(ctx: typer.Context) -> None:
        """Commands to download data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf data download --help[/] for more information.")
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            selected_tables = [RawTableId(db_name=database, name=table) for table in tables]
        elif tables and not database:
            raise typer.BadParameter(
                "The '--database' option is required when specifying tables as arguments.",
                param_hint="--database",
            )
        elif not tables and database:
            selected_tables = RawTableInteractiveSelect(client, "download").select_tables(database=database)
        else:
            selected_tables = RawTableInteractiveSelect(client, "download").select_tables()

        cmd.run(
            lambda: cmd.download(
                selectors=[
                    RawTableSelector(
                        table=SelectedTable(db_name=item.db_name, table_name=item.name),
                        download_dir_name=item.db_name,
                    )
                    for item in selected_tables
                ],
                io=RawIO(client, api_format=api_format.value),
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            data_sets, file_format, compression, output_dir, limit, api_format = self._asset_centric_interactive(
                AssetInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "assets",
            )

        selectors = [
            DataSetSelector(
                kind="Assets",
                data_set_external_id=data_set,
                download_dir_name="asset-centric-assets",
            )
            for data_set in data_sets
        ]
        cmd = DownloadCommand(client=client)
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=AssetDataIO(client, api_format=api_format.value),
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
    ) -> tuple[list[str], AssetCentricFormats, CompressionFormat, Path, int, ApiFormat]:
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
        api_format = ApiFormat.request
        if Flags.EXTEND_DOWNLOAD.is_enabled():
            api_format = questionary.select(
                message=f"Select the API format to download the {display_name}:",
                choices=[
                    Choice(title="Request payload format", value=ApiFormat.request),
                    Choice(title="API response format", value=ApiFormat.response),
                ],
                default=ApiFormat.request,
            ).unsafe_ask()
        return data_sets, file_format, compression, output_dir, limit, api_format

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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            data_sets, file_format, compression, output_dir, limit, api_format = self._asset_centric_interactive(
                TimeSeriesInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "time series",
            )

        selectors = [
            DataSetSelector(
                kind="TimeSeries", data_set_external_id=data_set, download_dir_name="asset-centric-timeseries"
            )
            for data_set in data_sets
        ]
        cmd = DownloadCommand(client=client)
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=TimeSeriesDataIO(client, api_format=api_format.value),
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            data_sets, file_format, compression, output_dir, limit, api_format = self._asset_centric_interactive(
                EventInteractiveSelect(client, "download"),
                file_format,
                compression,
                output_dir,
                limit,
                "events",
            )

        selectors = [
            DataSetSelector(kind="Events", data_set_external_id=data_set, download_dir_name="asset-centric-events")
            for data_set in data_sets
        ]
        cmd = DownloadCommand(client=client)

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=EventDataIO(client, api_format=api_format.value),
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
                "only download 100 files at a time.",
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            if not include_file_contents:
                # Continue with regular interactive selection
                data_sets, file_format, compression, output_dir, limit, api_format = self._asset_centric_interactive(
                    FileMetadataInteractiveSelect(client, "download"),
                    file_format,
                    compression,
                    output_dir,
                    limit if not include_file_contents else 1000,
                    "file metadata",
                    max_limit=1000 if include_file_contents else None,
                    available_formats=AssetCentricFormats,
                )
        io: DataIO
        selectors: list[DataSelector]
        if include_file_contents:
            selector = DocumentsInteractiveSelect(client, max_selected=100)
            file_format = questionary.select(
                "Select format for the downloaded file metadata:",
                choices=[Choice(title=format_.value, value=format_) for format_ in AssetCentricFormats],
                default=file_format,
            ).unsafe_ask()
            output_dir = Path(
                questionary.path(
                    "Where to download the file metadata and contents:", default=str(output_dir), only_directories=True
                ).unsafe_ask()
            )
            selected = selector.select_documents()
            if selected.selection.file_type == "dms":
                download_dir_name = "cognite-file-with-content"
                io = CogniteFileContentIO(
                    client,
                    config_directory=output_dir / download_dir_name,
                    file_directory=output_dir / download_dir_name / "files",
                    api_format=api_format.value,
                )
                selectors = [
                    CogniteFileFilesSelectorV2(
                        download_dir_name=download_dir_name,
                        ids=tuple(
                            NodeWithNameId(
                                space=doc.instance_id.space,
                                external_id=doc.instance_id.external_id,
                                name=doc.source_file.name,
                            )
                            for doc in selected.documents
                            if doc.instance_id
                        ),
                    )
                ]
            else:
                download_dir_name = "asset-centric-files-with-content"
                io = FileMetadataContentIO(
                    client,
                    config_directory=output_dir / download_dir_name,
                    file_directory=output_dir / download_dir_name / "files",
                    api_format=api_format.value,
                )
                selectors = [
                    FileMetadataFilesSelectorV2(
                        ids=tuple(
                            InternalWithNameId(id=document.id, name=document.source_file.name)
                            for document in selected.documents
                        ),
                        download_dir_name=download_dir_name,
                    )
                ]
        elif data_sets is not None:
            selectors = [
                DataSetSelector(
                    kind="FileMetadata", data_set_external_id=data_set, download_dir_name="asset-centric-files"
                )
                for data_set in data_sets
            ]
            io = FileMetadataDataIO(client, api_format=api_format.value)
        else:
            raise NotImplementedError("Bug in Toolkit. Unexpected execution path.")

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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            AssetSubtreeSelector(
                hierarchy=hierarchy,
                kind=resource_type,  # type: ignore[arg-type]
                download_dir_name=f"hierarchy-{sanitize_filename(hierarchy)}",
            )
            for resource_type in ["Assets", "Events", "FileMetadata", "TimeSeries"]
        ]
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=HierarchyIO(client, api_format=api_format.value),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @classmethod
    def download_instances_cmd(
        cls,
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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

        selectors: list[InstanceSelector]
        if instance_space is None:
            selector = DataModelingSelect(client, "download instances")
            data_model = selector.select_data_model(
                inline_views=True,
                message="Select the data model through which to download instances:",
                include_global=True,
            )
            selected_views = selector.select_view(
                multiselect=True,
                message="Select views to download instance properties from.",
                filter=ViewSelectFilter(
                    strategy="dataModel",
                    include_global=True,
                    data_model=data_model.as_id(),
                ),
            )
            select_instance_space = questionary.confirm(
                "Do you want to select an instance space to download from? If no, all instances from the selected views will be downloaded.",
                default=False,
            ).unsafe_ask()
            instance_spaces: tuple[str, ...] | None = None
            if select_instance_space:
                instance_spaces = tuple(selector.select_instance_space(multiselect=True))
            edge_type_ids_by_view_id: dict[ViewId, set[EdgeTypeId]] = {}
            if Flags.EXTEND_DOWNLOAD.EXTEND_DOWNLOAD.is_enabled():
                include_edges = questionary.confirm(
                    "Do you want to include edges when downloading node instances? If yes, all edges connected to the downloaded nodes will be downloaded as well.",
                    default=False,
                ).unsafe_ask()
                if include_edges:
                    for view in data_model.views or []:
                        view_id = view.as_id()
                        for prop in view.properties.values():
                            if isinstance(prop, EdgeProperty):
                                edge_type_ids_by_view_id.setdefault(view_id, set()).add(prop.as_edge_type_id())

            selectors = []
            download_dir_name = sanitize_filename(data_model.external_id)
            for view in selected_views:
                view_instance_type = selector.select_instance_type(
                    view.used_for,
                    message=f"Select instance type to download for view {view.space}:{view.external_id}(version={view.version})",
                )
                edge_types = edge_type_ids_by_view_id.get(view.as_id())

                selectors.append(
                    InstanceViewSelector(
                        view=SelectedView(
                            space=view.space,
                            external_id=view.external_id,
                            version=view.version,
                        ),
                        instance_spaces=instance_spaces,
                        instance_type=view_instance_type,
                        download_dir_name=download_dir_name,
                        edge_types=tuple(edge_types) if edge_types else None,
                    )
                )
            output_dir, file_format, compression, limit = cls._interactive_select_shared(  # type: ignore[assignment]
                output_dir, file_format, InstanceFormats, compression, limit, "instances", "view"
            )
        elif schema_space is None and view_external_ids is None:
            selectors = [
                InstanceSpaceSelector(
                    instance_space=instance_space,
                    instance_type=instance_type.value,
                    download_dir_name=instance_space,
                )
            ]
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
                    download_dir_name=instance_space,
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
                io=InstanceIO(client, api_format=api_format.value),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @classmethod
    def _interactive_select_shared(
        cls,
        output_dir: Path,
        file_format: Enum,
        file_format_options: type[Enum],
        compression: CompressionFormat,
        limit: int,
        display_name: str,
        selector_type: str,
        max_limit: int | None = None,
    ) -> tuple[Path, Enum, Enum, int]:
        """Interactive selection of output_dir, file_format, compression and limit for the download commands."""
        selected_output_dir = Path(
            questionary.path("Where to download the data:", default=str(output_dir), only_directories=True).unsafe_ask()
        )

        file_formats = [Choice(title=format_.value, value=format_) for format_ in file_format_options]
        if len(file_formats) == 1:
            selected_file_format = file_formats[0].value
        else:
            selected_file_format = questionary.select(
                "Select format to download the data in:",
                choices=file_formats,
                default=file_format,  # type: ignore[arg-type]
            ).unsafe_ask()

        selected_compression = questionary.select(
            "Select compression format to use when downloading the data:",
            choices=[Choice(title=comp.value, value=comp) for comp in CompressionFormat],
            default=compression,
        ).unsafe_ask()
        limit_prompt = f"The maximum number of {display_name} to download per {selector_type}. "
        if max_limit is not None:
            limit_prompt += f"Use -1 to download up to the maximum of {max_limit:,} {display_name}."
        else:
            limit_prompt += f"Use -1 to download all {display_name}."
        selected_limit = int(
            questionary.text(
                limit_prompt,
                default=str(limit),
                validate=lambda value: value.lstrip("-").isdigit() and (int(value) == -1 or int(value) > 0),
            ).unsafe_ask()
        )
        return selected_output_dir, selected_file_format, selected_compression, selected_limit  # type: ignore[return-value]

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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            download_dir_name=f"dataset-{dataset}",
        )
        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=DatapointsIO(client, api_format=api_format.value),
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
        skip_backend_services: Annotated[
            bool,
            typer.Option(
                "--skip-backend-services",
                help="Skip downloading backend-services for charts, i.e., monitoring jobs and scheduled calculations.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = not Flags.EXTEND_DOWNLOAD.is_enabled(),
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
            selector = ChartExternalIdSelector(external_ids=tuple(selected_external_ids), download_dir_name="charts")
        else:
            selector = ChartExternalIdSelector(external_ids=tuple(external_ids), download_dir_name="charts")

        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=ChartIO(client, skip_backend_services=skip_backend_services, api_format=api_format.value),
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
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
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
            selector = CanvasExternalIdSelector(external_ids=tuple(selected_external_ids), download_dir_name="canvas")
        else:
            selector = CanvasExternalIdSelector(external_ids=tuple(external_ids), download_dir_name="canvas")

        cmd.run(
            lambda: cmd.download(
                selectors=[selector],
                io=CanvasIO(client, api_format=api_format.value),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @classmethod
    def download_records_cmd(
        cls,
        stream: Annotated[
            str | None,
            typer.Option(
                "--stream",
                "-s",
                help="The external ID of the stream to download records from. "
                "If not provided, an interactive selection will be made.",
            ),
        ] = None,
        instance_spaces: Annotated[
            list[str] | None,
            typer.Option(
                "--instance-space",
                help="Only download records belonging to these spaces. "
                "Can be specified multiple times. If not provided, records from all spaces will be included.",
            ),
        ] = None,
        containers: Annotated[
            list[str] | None,
            typer.Option(
                "--container",
                "-c",
                help="Containers to download record properties from, in 'space:externalId' format. "
                "Can be specified multiple times to download records from multiple containers.",
            ),
        ] = None,
        initialize_cursor: Annotated[
            str,
            typer.Option(
                "--initialize-cursor",
                help="Controls where to start reading changes from the stream. "
                "The format is 'duration-ago', where 'duration' is a correct duration "
                "representation: 3m, 400h, 25d, etc. For instance, '2d-ago' will give "
                "changes ingested up to 2 days ago.",
            ),
        ] = "365d-ago",
        file_format: Annotated[
            RecordFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the records in.",
            ),
        ] = RecordFormats.ndjson,
        api_format: Annotated[
            ApiFormat,
            typer.Option(
                "--api-format",
                help="API communication format. 'request' uses the request payload format, 'response' uses the API response format.",
                hidden=not Flags.EXTEND_DOWNLOAD.is_enabled(),
            ),
        ] = ApiFormat.request,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the records.",
            ),
        ] = CompressionFormat.gzip,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the records.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help=f"The maximum number of records to download per container. Use -1 to download up to the maximum of {RecordIO.MAX_TOTAL_RECORDS} records.",
                max=RecordIO.MAX_TOTAL_RECORDS,
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
        """This command will download Records from a CDF stream into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DownloadCommand(client=client)

        selectors: list[RecordContainerSelector]
        if stream is None and containers is None:
            record_select = RecordInteractiveSelect(client)
            selected_stream = record_select.select_stream()
            selected_containers = record_select.select_containers()

            download_dir_name = sanitize_filename(selected_stream.external_id)

            selected_instance_spaces: tuple[str, ...] | None = None
            if instance_spaces:
                selected_instance_spaces = tuple(instance_spaces)
            else:
                select_instance_space = questionary.confirm(
                    "Do you want to filter records by space? If no, records from all spaces will be downloaded.",
                    default=False,
                ).unsafe_ask()
                if select_instance_space:
                    selected_instance_spaces = tuple(record_select.select_instance_spaces())
            selected_initialize_cursor = record_select.select_initialize_cursor(default=initialize_cursor)
            selectors = [
                RecordContainerSelector(
                    stream=SelectedStream(external_id=selected_stream.external_id),
                    container=SelectedContainer(space=container.space, external_id=container.external_id),
                    instance_spaces=selected_instance_spaces,
                    initialize_cursor=selected_initialize_cursor,
                    download_dir_name=download_dir_name,
                )
                for container in selected_containers
            ]
            output_dir, file_format, compression, limit = cls._interactive_select_shared(  # type: ignore[assignment]
                output_dir,
                file_format,
                RecordFormats,
                compression,
                limit,
                "records",
                "container",
                max_limit=RecordIO.MAX_TOTAL_RECORDS,
            )
        elif stream is not None and containers is not None:
            selected_instance_spaces = tuple(instance_spaces) if instance_spaces else None
            parsed_containers: list[SelectedContainer] = []
            for container_str in containers:
                parts = container_str.split(":", 1)
                if len(parts) != 2 or not all(parts):
                    raise typer.BadParameter(
                        f"Invalid container format: {container_str!r}. Expected 'space:externalId'.",
                        param_hint="--container",
                    )
                parsed_containers.append(SelectedContainer(space=parts[0], external_id=parts[1]))
            selectors = [
                RecordContainerSelector(
                    stream=SelectedStream(external_id=stream),
                    container=container,
                    instance_spaces=selected_instance_spaces,
                    initialize_cursor=initialize_cursor,
                    download_dir_name=sanitize_filename(stream),
                )
                for container in parsed_containers
            ]
        else:
            raise typer.BadParameter(
                "Both '--stream' and '--container' must be provided together.",
                param_hint="--stream / --container",
            )

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=RecordIO(client, api_format=api_format.value),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )
