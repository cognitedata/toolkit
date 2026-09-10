from typing import Any

import typer

from ._helpers import print_help_if_no_subcommand

DEFAULT_LIST_LIMIT = 25


class APIApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(InstancesApp(*args, **kwargs), name="instances")

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Plugin to work with data in CDF"""
        print_help_if_no_subcommand(ctx)


class InstancesApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Plugin to work with data in CDF"""
        print_help_if_no_subcommand(ctx)

    @staticmethod
    def list(
        view: str | None = typer.Option(
            None,
            "--view",
            "-v",
            help="The properties to get for each instance. If not specified, only the instance properties are returned - not any container properties.",
        ),
        filter: str | None = typer.Option(
            None,
            "--filter",
            "-f",
            help="A filter expression to filter the instances. If not specified, all instances are returned.",
        ),
        limit: int = typer.Option(
            DEFAULT_LIST_LIMIT,
            "--limit",
            "-l",
            help="Maximum number of instances to return. If not specified, the default limit is 25.",
            max=1000,
        ),
    ) -> None:
        """List instances in CDF"""
        ...
