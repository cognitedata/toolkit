from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands.functions import FunctionsCommand

CDF_TOML = CDFToml.load(Path.cwd())


class FunctionsApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.init)

    def main(self, ctx: typer.Context) -> None:
        """Commands for managing Cognite Function Apps"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf functions --help[/] for more information.")

    def init(
        self,
        organization_dir: Annotated[
            Optional[Path],
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to the organization directory. Defaults to the value in cdf.toml.",
                exists=True,
                file_okay=False,
                dir_okay=True,
                resolve_path=True,
            ),
        ] = None,
        module: Annotated[
            Optional[str],
            typer.Option(
                "--module",
                "-m",
                help="Module to scaffold the function into. Prompts if omitted.",
            ),
        ] = None,
        external_id: Annotated[
            Optional[str],
            typer.Option(
                "--external-id",
                "-e",
                help="Function externalId. Prompts if omitted.",
            ),
        ] = None,
        name: Annotated[
            Optional[str],
            typer.Option(
                "--name",
                "-n",
                help="Display name for the function. Prompts if omitted.",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command.",
            ),
        ] = False,
    ) -> None:
        """Scaffold a cognite-function-apps Function App into an existing module.

        Creates three files inside <organization_dir>/<module>/functions/:
          <external_id>.Function.yaml
          <external_id>/handler.py
          <external_id>/requirements.txt
        """
        resolved_org_dir = organization_dir or CDF_TOML.cdf.default_organization_dir
        cmd = FunctionsCommand()
        cmd.run(
            lambda: cmd.init(
                organization_dir=resolved_org_dir,
                module_name=module,
                external_id=external_id,
                name=name,
                verbose=verbose,
            )
        )
