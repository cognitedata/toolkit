from pathlib import Path
from typing import Annotated, Any, Optional, Union

import typer
from cognite.client.data_classes.data_modeling import DataModelId
from rich import print

from cognite_toolkit._cdf_tk.commands import DumpAssetsCommand, DumpCommand, DumpTimeSeriesCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
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
        data_model_id: Annotated[
            Optional[list[str]],
            typer.Argument(
                help="Data model ID to dump. Format: space external_id version. Example: 'my_space my_external_id v1'. "
                "Note that version is optional and defaults to the latest published version. If nothing is provided,"
                "an interactive prompt will be shown to select the data model.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the datamodel files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the datamodel.",
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
        """This command will dump the selected data model as yaml to the folder specified, defaults to /tmp."""
        selected_data_model: Union[DataModelId, None] = None
        if data_model_id is not None:
            if len(data_model_id) <= 2:
                raise ToolkitRequiredValueError(
                    "Data model ID must have at least 2 parts: space, external_id, and, optionally, version."
                )
            selected_data_model = DataModelId(*data_model_id)

        cmd = DumpCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                selected_data_model,
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
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted"
                "to select which assets to dump",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the assets in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            Optional[int],
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
        cmd = DumpAssetsCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                hierarchy,
                data_set,
                output_dir,
                clean,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )

    def dump_timeseries_cmd(
        self,
        ctx: typer.Context,
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If not provided, the user will be prompted to select which timeseries to dump.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the timeseries in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            Optional[int],
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
        cmd = DumpTimeSeriesCommand()
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                data_set,
                None,
                output_dir,
                clean,
                limit,
                format_,  # type: ignore [arg-type]
                verbose,
            )
        )
