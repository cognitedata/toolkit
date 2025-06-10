from pathlib import Path
from typing import Annotated, Any, Optional

import typer


class MigrateApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("timeseries")(self.timeseries)

    def main(self, ctx: typer.Context) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf migrate --help[/] for more information.")

    @staticmethod
    def timeseries(
        ctx: typer.Context,
        mapping_file: Annotated[
            Optional[Path],
            typer.Argument(
                help="Path to the mapping file that contains the mapping from TimeSeries to CogniteTimeSeries. "
                "Note you cannot provide a mapping file and data sets at the same time. If neither is provided"
                ", interactive mode will be used.",
            ),
        ],
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-sets",
                "-d",
                help="List of data sets to migrate. If not provided, interactive mode will be used. "
                "Note you cannot provide a mapping file and data sets at the same time. If neither is provided"
                ", interactive mode will be used.",
            ),
        ] = None,
        dry_run: Annotated[
            Optional[bool],
            typer.Argument(
                help="If set, the migration will not be executed, but only a report of what would be done is printed.",
            ),
        ] = False,
        link: Annotated[
            bool,
            typer.Option(
                "--link",
                "-l",
                help="By default, the migration will link the migrated CogniteTimeSeries to the original TimeSeries.",
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
        """Migrate TimeSeries to CogniteTimeSeries."""
        raise NotImplementedError()
