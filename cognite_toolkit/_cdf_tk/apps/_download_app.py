from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.storageio import ChartIdSelector, ChartIO, ChartSelector, RawIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import InteractiveChartSelect, RawTableInteractiveSelect


class RawFormats(str, Enum):
    ndjson = "ndjson"
    yaml = "yaml"


class ChartFormats(str, Enum):
    ndjson = "ndjson"


class CompressionFormat(str, Enum):
    gzip = "gzip"
    none = "none"


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.download_main)
        self.command("charts")(self.download_chart_cmd)
        self.command("raw")(self.download_raw_cmd)

    @staticmethod
    def download_main(ctx: typer.Context) -> None:
        """Commands to download data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf download --help[/] for more information.")
        return None

    @staticmethod
    def download_chart_cmd(
        ctx: typer.Context,
        chart_ids: Annotated[
            list[str] | None,
            typer.Argument(
                help="List of chart IDs to download. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            ChartFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the charts in. Supported formats: ndjson",
            ),
        ] = ChartFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the charts. Supported formats: gzip, none.",
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
        """This command will download charts from CDF into a temporary directory."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        selectors: list[ChartSelector]
        if chart_ids:
            selectors = [ChartIdSelector(chart_ids=set(chart_ids))]
        else:
            interactive = InteractiveChartSelect(client, "download")
            selectors = [ChartIdSelector(set(interactive.select_external_ids()))]

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=ChartIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=None,
                verbose=verbose,
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
        ] = Path("tmp"),
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
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
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
                selectors=selectors,
                io=RawIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )
