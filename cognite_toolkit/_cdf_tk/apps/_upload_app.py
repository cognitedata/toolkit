from pathlib import Path
from typing import Annotated, Any

import questionary
import typer
from questionary import Choice

from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_DEFAULT_DIR, DATA_MANIFEST_SUFFIX, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

DEFAULT_INPUT_DIR = Path.cwd() / DATA_DEFAULT_DIR


class UploadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.command("dir")(self.upload_main)

    @staticmethod
    def upload_main(
        ctx: typer.Context,
        input_dir: Annotated[
            Path | None,
            typer.Argument(
                help="The directory containing the data to upload. If not specified, an interactive prompt will ask for the directory.",
                exists=True,
                file_okay=False,
                dir_okay=True,
                resolve_path=True,
            ),
        ] = None,
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
        """Commands to upload data to CDF."""
        cmd = UploadCommand()
        if input_dir is None:
            input_candidate = sorted([p.parent for p in DEFAULT_INPUT_DIR.rglob(f"*/**{DATA_MANIFEST_SUFFIX}")])
            if not input_candidate:
                typer.echo(f"No data manifests found in default directory: {DEFAULT_INPUT_DIR}")
                raise typer.Exit(code=1)
            input_dir = questionary.select(
                "Select the input directory containing the data to upload:",
                choices=[Choice(str(option.name), value=option) for option in input_candidate],
            ).ask()
            if input_dir is None:
                typer.echo("No input directory selected. Exiting.")
                raise typer.Exit(code=1)
            dry_run = questionary.confirm("Proceed with dry run?", default=dry_run).ask()
            if dry_run is None:
                typer.echo("No selection made for dry run. Exiting.")
                raise typer.Exit(code=1)
            if (input_dir / DATA_RESOURCE_DIR).exists():
                deploy_resources = questionary.confirm(
                    "Deploy resources found in adjacent folders?", default=deploy_resources
                ).ask()
                if deploy_resources is None:
                    typer.echo("No selection made for deploying resources. Exiting.")
                    raise typer.Exit(code=1)

            client = EnvironmentVariables.create_from_environment().get_client()
            cmd.run(
                lambda: cmd.upload(
                    input_dir=input_dir,
                    dry_run=dry_run,
                    verbose=verbose,
                    deploy_resources=deploy_resources,
                    client=client,
                )
            )
