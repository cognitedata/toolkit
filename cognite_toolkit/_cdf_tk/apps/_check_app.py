from pathlib import Path
from typing import Any

import typer

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml

from ._helpers import print_help_if_no_subcommand

CDF_TOML = CDFToml.load_module_context(Path.cwd())


class CheckApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to check Cognite Data Fusion."""
        print_help_if_no_subcommand(ctx)
