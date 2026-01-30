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
        self.callback(invoke_without_command=True)(self.upload_main)
        self.command("dir")(self.upload_dir)

    @staticmethod
    def upload_main(ctx: typer.Context) -> None:
        """Commands to upload data to CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf upload --help[/] for more information.")
        return None

    @staticmethod
    def upload_dir(
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
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = UploadCommand(client=client)
        if input_dir is None:
            input_candidate = sorted({p.parent for p in DEFAULT_INPUT_DIR.rglob(f"**/*{DATA_MANIFEST_SUFFIX}")})
            if not input_candidate:
                typer.echo(f"No data manifests found in default directory: {DEFAULT_INPUT_DIR}")
                raise typer.Exit(code=1)
            input_dir = questionary.select(
                "Select the input directory containing the data to upload:",
                choices=[Choice(str(option.name), value=option) for option in input_candidate],
            ).unsafe_ask()
            dry_run = questionary.confirm("Proceed with dry run?", default=dry_run).unsafe_ask()
            if dry_run is None:
                typer.echo("No selection made for dry run. Exiting.")
                raise typer.Exit(code=1)
            resource_dir = Path(input_dir) / DATA_RESOURCE_DIR
            if resource_dir.exists():
                if resource_dir.is_relative_to(Path.cwd()):
                    display_name = resource_dir.relative_to(Path.cwd()).as_posix()
                else:
                    display_name = resource_dir.as_posix()

                deploy_resources = questionary.confirm(
                    f"Deploy resources found in {display_name!r}?", default=deploy_resources
                ).unsafe_ask()

        cmd.run(
            lambda: cmd.upload(
                input_dir=input_dir,
                dry_run=dry_run,
                verbose=verbose,
                deploy_resources=deploy_resources,
                client=client,
            )
        )
