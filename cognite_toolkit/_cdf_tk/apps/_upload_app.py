from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class UploadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.upload_main)
        self.command("raw")(self.upload_raw_cmd)

    @staticmethod
    def upload_main(ctx: typer.Context) -> None:
        """Commands to upload data to CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf upload --help[/] for more information.")
        return None

    @staticmethod
    def upload_raw_cmd(
        ctx: typer.Context,
        input_dir: Annotated[
            Path,
            typer.Argument(
                help="The directory containing the RAW tables to upload. "
                "If not specified, the current working directory will be used.",
                exists=True,
                file_okay=False,
                dir_okay=True,
                resolve_path=True,
            ),
        ],
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-d",
                help="If set, the command will not actually upload the data, but will print what would be uploaded.",
            ),
        ] = False,
        ensure_tables: Annotated[
            bool,
            typer.Option(
                "--ensure-tables",
                "-e",
                help="If set, the command will ensure that the RAW database and table exist in CDF before uploading.",
            ),
        ] = True,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will upload data from the input directory to CDF."""
        cmd = UploadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.upload(
                io=RawIO(client=client),
                input_dir=input_dir,
                ensure_configurations=ensure_tables,
                dry_run=dry_run,
                verbose=verbose,
            )
        )
