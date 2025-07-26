from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.commands.download import (
    AssetFinder,
    EventFinder,
    FileMetadataFinder,
    TimeSeriesFinder,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    TimeSeriesInteractiveSelect,
)


class DownloadFormat(str, Enum):
    csv = "csv"
    parquet = "parquet"


class RecordFormat(str, Enum):
    ndjson = "ndjson"


class CompressionFormat(str, Enum):
    gzip = "gzip"
    none = "none"


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.download_main)
        self.command("asset")(self.download_asset_cmd)
        self.command("files-metadata")(self.download_files_cmd)
        self.command("timeseries")(self.download_timeseries_cmd)
        self.command("event")(self.download_event_cmd)
        self.command("raw")(self.download_raw_cmd)

    @staticmethod
    def download_main(ctx: typer.Context) -> None:
        """Commands to download data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf download --help[/] for more information.")
        return None

    @staticmethod
    def download_asset_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Hierarchy to download.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to download. If neither hierarchy nor data set is provided, the user will be prompted"
                "to select which assets to download",
            ),
        ] = None,
        format_: Annotated[
            DownloadFormat,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the assets in. Supported formats: csv, and parquet.",
            ),
        ] = DownloadFormat.csv,
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of assets to download.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the asset files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before downloading the asset.",
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
        """This command will download the selected assets in the selected format to the folder specified, defaults to /tmp."""
        cmd = DownloadCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = AssetInteractiveSelect(client, "download").interactive_select_hierarchy_datasets()

        cmd.run(
            lambda: cmd.download_table(
                AssetFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_.value,
                verbose,
            )
        )

    @staticmethod
    def download_files_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to download filemetadata from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to download. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            DownloadFormat,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the filemetadata in. Supported formats: csv, and parquet.",
            ),
        ] = DownloadFormat.csv,
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of filemetadata to download.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the filemetadata files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before downloading the filemetadata.",
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
        """This command will download the selected file-metadata in the selected format in the folder specified, defaults to /tmp."""
        cmd = DownloadCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = FileMetadataInteractiveSelect(
                client, "download"
            ).interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.download_table(
                FileMetadataFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_.value,
                verbose,
            )
        )

    @staticmethod
    def download_timeseries_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to download timeseries from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to download. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            DownloadFormat,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the timeseries in. Supported formats: csv, and parquet.",
            ),
        ] = DownloadFormat.csv,
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of timeseries to download.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the timeseries files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before downloading the timeseries.",
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
        """This command will download the selected timeseries to the selected format in the folder specified, defaults to /tmp."""
        cmd = DownloadCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = TimeSeriesInteractiveSelect(
                client, "download"
            ).interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.download_table(
                TimeSeriesFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_.value,
                verbose,
            )
        )

    @staticmethod
    def download_event_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to download event from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to download. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            DownloadFormat,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the event in. Supported formats: csv, and parquet.",
            ),
        ] = DownloadFormat.csv,
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of events to download.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the events files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before downloading the events.",
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
        """This command will download the selected events to the selected format in the folder specified, defaults to /tmp."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = EventInteractiveSelect(client, "download").interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.download_table(
                EventFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_.value,
                verbose,
            )
        )

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
        format_: Annotated[
            RecordFormat,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the raw tables in. Supported formats: json",
            ),
        ] = RecordFormat.ndjson,
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
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before downloading the raw tables.",
            ),
        ] = False,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of records to download from each table.",
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
        """This command will download RAW tables from CDF into a temporary directory."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.download_raw_records(
                client=client,
                database=database,
                tables=tables,
                output_dir=output_dir,
                format_=format_,
                compression=compression,
                clean=clean,
                limit=limit,
                verbose=verbose,
            )
        )
