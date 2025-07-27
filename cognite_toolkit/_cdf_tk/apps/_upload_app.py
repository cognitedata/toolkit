from pathlib import Path
from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.commands.clean import AVAILABLE_DATA_TYPES


class UploadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        # Add when the build command is implemented
        # self.command("build")(self.build)
        self.command("raw")(self.raw)

    def main(self, ctx: typer.Context) -> None:
        """Commands to upload data to CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf upload --help[/] for more information.")

    @staticmethod
    def build(
        ctx: typer.Context,
        build_dir: Annotated[
            Path,
            typer.Argument(
                help="Path to the build directory, defaults to '/build'.",
                allow_dash=True,
            ),
        ] = Path("/build"),
        build_env_name: Annotated[
            str | None,
            typer.Option(
                "--env",
                "-e",
                help="CDF project environment to use for deployment. This is optional and "
                "if passed it is used to verify against the build environment",
            ),
        ] = None,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, no data will be uploaded, but the upload process will be simulated.",
            ),
        ] = False,
        include: Annotated[
            list[str] | None,
            typer.Option(
                "--include",
                help=f"Specify which resources to deploy, available options: {AVAILABLE_DATA_TYPES}.",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="If set, the command will print more information about the upload process.",
            ),
        ] = False,
    ) -> None:
        """Uploads data that is referenced in the build directory to obtained with `cdf build`."""
        raise NotImplementedError()

    @staticmethod
    def raw(
        ctx: typer.Context,
        input_data: Annotated[
            Path | None,
            typer.Argument(
                help="Path to the raw data file to upload or directory to search for raw configurations with data. "
                "If not provided, interactive mode will be used.",
            ),
        ] = None,
        database: Annotated[
            str | None,
            typer.Option(
                "--database",
                "-d",
                help="The database to upload the raw data to. If not provided, Toolkit will look for a database configuration file.",
            ),
        ] = None,
        table: Annotated[
            str | None,
            typer.Option(
                "--table",
                "-t",
                help="The table to upload the raw data to. If not provided, Toolkit will look for a table configuration file.",
            ),
        ] = None,
    ) -> None:
        """This command lets you upload raw data directly to CDF."""
        raise NotImplementedError()
