from pathlib import Path
from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_DEFAULT_DIR
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

DEFAULT_INPUT_DIR = Path.cwd() / DATA_DEFAULT_DIR


class UploadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.upload_main)

    @staticmethod
    def upload_main(
        ctx: typer.Context,
        input_dir: Annotated[
            Path,
            typer.Argument(
                help="The directory containing the data to upload.",
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
        deploy_resources: Annotated[
            bool,
            typer.Option(
                "--deploy-resources",
                "-r",
                help="If set, the command will look for resource configuration files in adjacent folders and create them if they do not exist.",
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
        """Commands to upload data to CDF."""
        cmd = UploadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        cmd.run(
            lambda: cmd.upload(
                input_dir=input_dir,
                dry_run=dry_run,
                verbose=verbose,
                deploy_resources=deploy_resources,
                io=None,
                client=client,
            )
        )
