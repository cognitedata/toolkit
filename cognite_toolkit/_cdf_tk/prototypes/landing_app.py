from __future__ import annotations

import typer
from rich import print
from rich.panel import Panel


class Landing(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.main)

    def main(self) -> None:
        print(
            Panel("""Welcome to the Cognite Toolkit!
                    \n1. Run `cdf-tk repo init (folder_name)` to initialize a new CDF project
                    \n2. Run `cdf-tk modules (folder_name)` to select modules."))
                    \n3. Run `cdf-tk auth verify [--interactive] (folder_name)` to check access"
                    \n4. Run `cdf-tk build
                    \n5. Run `cdf-tk deploy --dry-run` to simulate deployment
                    "))""")
        )
