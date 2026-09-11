from enum import Enum
from typing import Any

import typer

from cognite_toolkit._cdf_tk.apps._helpers import print_help_if_no_subcommand
from cognite_toolkit._cdf_tk.commands import InstancesAPICommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

DEFAULT_LIST_LIMIT = 25


class InstanceTypeEnum(str, Enum):
    node = "node"
    edge = "edge"


class APIApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(InstancesApp(*args, **kwargs), name="instances")

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to call CDF APIs"""
        print_help_if_no_subcommand(ctx)


class InstancesApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.list)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with data modeling instances"""
        print_help_if_no_subcommand(ctx)

    @staticmethod
    def list(
        view: str = typer.Option(
            "cdf_cdm:CogniteDescribable/v1",
            "--view",
            "-v",
            help="The properties to get for each instance, given as 'space:externalId/version'. ",
        ),
        filter: str | None = typer.Option(
            None,
            "--filter",
            "-f",
            help="A YAML/JSON filter expression to filter the instances.",
        ),
        limit: int = typer.Option(
            DEFAULT_LIST_LIMIT,
            "--limit",
            "-l",
            help="Maximum number of instances to return.",
            max=1000,
        ),
        instance_type: InstanceTypeEnum = typer.Option(
            InstanceTypeEnum.node,
            "--instance-type",
            "-t",
            help="Type of instances to list. Can be 'node' or 'edge'.",
            case_sensitive=False,
        ),
    ) -> None:
        """List instances in CDF"""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = InstancesAPICommand()
        cmd.run(lambda: cmd.list(client, view=view, filter=filter, limit=limit, instance_type=instance_type.value))
