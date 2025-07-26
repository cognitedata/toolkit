from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import DumpDataCommand
from cognite_toolkit._cdf_tk.commands.dump_data import (
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


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_data_main)
        self.command("asset")(self.dump_asset_cmd)
        self.command("files-metadata")(self.dump_files_cmd)
        self.command("timeseries")(self.dump_timeseries_cmd)
        self.command("event")(self.dump_event_cmd)

    @staticmethod
    def dump_data_main(ctx: typer.Context) -> None:
        """Commands to dump data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump data --help[/] for more information.")
        return None

    @staticmethod
    def dump_asset_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Hierarchy to dump.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted"
                "to select which assets to dump",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the assets in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of assets to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the asset files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the asset.",
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
        """This command will dump the selected assets in the selected format to the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = AssetInteractiveSelect(client, "dump").interactive_select_hierarchy_datasets()

        cmd.run(
            lambda: cmd.dump_table(
                AssetFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )

    @staticmethod
    def dump_files_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump filemetadata from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the filemetadata in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of filemetadata to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the filemetadata files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the filemetadata.",
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
        """This command will dump the selected events to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        cmd.validate_directory(output_dir, clean)
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = FileMetadataInteractiveSelect(client, "dump").interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.dump_table(
                FileMetadataFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )

    @staticmethod
    def dump_timeseries_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump timeseries from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the timeseries in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of timeseries to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the timeseries files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the timeseries.",
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
        """This command will dump the selected timeseries to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = TimeSeriesInteractiveSelect(client, "dump").interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.dump_table(
                TimeSeriesFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )

    @staticmethod
    def dump_event_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump event from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the event in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of events to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the events files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the events.",
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
        """This command will dump the selected events to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        cmd.validate_directory(output_dir, clean)
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = EventInteractiveSelect(client, "dump").interactive_select_hierarchy_datasets()
        cmd.run(
            lambda: cmd.dump_table(
                EventFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,
                verbose,
            )
        )
