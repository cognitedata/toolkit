from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands.entity_matching import EntityMatchingCommand

CDF_TOML = CDFToml.load(Path.cwd())


class EntityMatchingApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("generate-aliasing-workflow")(self.generate_aliasing_workflow)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands for entity matching."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev entity-matching --help[/] for more information.")

    def generate_aliasing_workflow(
        self,
        input_yaml: Annotated[
            Path,
            typer.Argument(
                help="Path to the aliasing-rules YAML input file.",
                exists=True,
                file_okay=True,
                dir_okay=False,
                readable=True,
            ),
        ],
        output_dir: Annotated[
            Path | None,
            typer.Option(
                "--output-dir",
                "-o",
                help="Directory where the generated YAML files will be written. "
                "Defaults to a 'generated/' folder next to the input file.",
            ),
        ] = None,
    ) -> None:
        """Generate Workflow and WorkflowVersion YAML files from an aliasing-rules input file."""
        resolved_output_dir = output_dir or (input_yaml.parent / "generated")
        cmd = EntityMatchingCommand()
        cmd.run(lambda: cmd.generate_aliasing_workflow(input_yaml=input_yaml, output_dir=resolved_output_dir))
