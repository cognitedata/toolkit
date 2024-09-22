from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from cognite.client.data_classes.data_modeling import DataModelId
from rich import print

from cognite_toolkit._cdf_tk.commands import DumpAssetsCommand, DumpCommand, DumpTimeSeriesCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class DumpApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_main)
        self.command("datamodel")(self.dump_datamodel_cmd)
        self.command("asset")(self.dump_asset_cmd)
        self.command("timeseries")(self.dump_timeseries_cmd)

    def dump_main(self, ctx: typer.Context) -> None:
        """Commands to dump resource configurations from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump --help[/] for more information.")
        return None

    def dump_datamodel_cmd(
        self,
        ctx: typer.Context,
        space: Annotated[
            str,
            typer.Option(
                "--space",
                "-s",
                prompt=True,
                help="Space where the datamodel to pull can be found.",
            ),
        ],
        external_id: Annotated[
            str,
            typer.Option(
                "--external-id",
                "-e",
                prompt=True,
                help="External id of the datamodel to pull.",
            ),
        ],
        version: Annotated[
            Optional[str],
            typer.Option(
                "--version",
                "-v",
                help="Version of the datamodel to pull.",
            ),
        ] = None,
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before pulling the datamodel.",
            ),
        ] = False,
        output_dir: Annotated[
            Path,
            typer.Argument(
                help="Where to dump the datamodel YAML files.",
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
        """This command will dump the selected data model as yaml to the folder specified, defaults to /tmp."""
        cmd = DumpCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                DataModelId(space, external_id, version),
                output_dir,
                clean,
                verbose,
            )
        )

    def dump_asset_cmd(
        self,
        ctx: typer.Context,
        hierarchy: Annotated[
            Optional[list[str]],
            typer.Option(
                "--hierarchy",
                "-h",
                help="Hierarchy to dump.",
            ),
        ] = None,
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump.",
            ),
        ] = None,
        interactive: Annotated[
            bool,
            typer.Option(
                "--interactive",
                "-i",
                help="Will prompt you to select which assets hierarchies to dump.",
            ),
        ] = False,
        output_dir: Annotated[
            Path,
            typer.Argument(
                help="Where to dump the asset YAML files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the assets in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        clean_: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before pulling the assets.",
            ),
        ] = False,
        limit: Annotated[
            Optional[int],
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of assets to dump.",
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
        """This command will dump the selected assets as yaml to the folder specified, defaults to /tmp."""
        cmd = DumpAssetsCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                hierarchy,
                data_set,
                interactive,
                output_dir,
                clean_,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )

    def dump_timeseries_cmd(
        self,
        ctx: typer.Context,
        time_series_list: Annotated[
            Optional[list[str]],
            typer.Option(
                "--timeseries",
                "-t",
                help="Timeseries to dump.",
            ),
        ] = None,
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump.",
            ),
        ] = None,
        interactive: Annotated[
            bool,
            typer.Option(
                "--interactive",
                "-i",
                help="Will prompt you to select which timeseries to dump.",
            ),
        ] = False,
        output_dir: Annotated[
            Path,
            typer.Argument(
                help="Where to dump the timeseries YAML files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the timeseries in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        clean_: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before pulling the timeseries.",
            ),
        ] = False,
        limit: Annotated[
            Optional[int],
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of timeseries to dump.",
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
        """This command will dump the selected timeseries as yaml to the folder specified, defaults to /tmp."""
        cmd = DumpTimeSeriesCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                data_set,
                interactive,
                output_dir,
                clean_,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )
