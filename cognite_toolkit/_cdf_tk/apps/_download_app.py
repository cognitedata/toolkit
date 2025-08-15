from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import RawTableInteractiveSelect


class RawFormats(str, Enum):
    ndjson = "ndjson"
    yaml = "yaml"


class CompressionFormat(str, Enum):
    gzip = "gzip"
    none = "none"


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.download_main)
        self.command("raw")(self.download_raw_cmd)

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
                help="Format to download the raw tables in. Supported formats: json",
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
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of records to download from each table.",
            ),
        ] = None,
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
        if database is None or tables is None:
            identifiers = RawTableInteractiveSelect(client, "download").select_tables()
        else:
            identifiers = [RawTable(db_name=database, table_name=table) for table in tables]
        cmd.run(
            lambda: cmd.download(
                identifiers=identifiers,
                io=RawIO(client),
                output_dir=output_dir,
                file_format=file_format.value,
                compression=compression.value,
                limit=limit,
                verbose=verbose,
            )
        )
