from enum import StrEnum
from typing import Any

import typer

from cognite_toolkit._cdf_tk.apps._helpers import print_help_if_no_subcommand
from cognite_toolkit._cdf_tk.commands import FunctionsAPICommand, InstancesAPICommand
from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables

DEFAULT_LIST_LIMIT = 25


class InstanceTypeEnum(StrEnum):
    node = "node"
    edge = "edge"


class APIApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(FunctionsApp(*args, **kwargs), name="functions")
        self.add_typer(InstancesApp(*args, **kwargs), name="instances")

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to call CDF APIs"""
        print_help_if_no_subcommand(ctx)


class FunctionsApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command()(self.retrieve)
        self.command()(self.status)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with Cognite Functions

        Calls the Cognite Functions API directly, without building or deploying any local modules. Authentication is read from the environment, the same way as for the other `cdf` commands.

        \b
        Examples:
          cdf api functions retrieve --external-id my_function
          cdf api functions status
        """
        print_help_if_no_subcommand(ctx)

    @staticmethod
    def retrieve(
        external_id: str = typer.Option(
            ...,
            "--external-id",
            "-e",
            help="External ID of the function to retrieve.",
        ),
    ) -> None:
        """Retrieve a function, including its deployment status

        Prints the function as returned by CDF. The `status` field is one of Queued, Deploying, Ready, Failed or Retired. If the deployment failed, the `error` field contains the reason. The command fails if the function does not exist.

        \b
        Examples:
          Check whether a function is ready to be called:
          cdf api functions retrieve --external-id my_function

        \b
          Short form:
          cdf api functions retrieve -e my_function
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = FunctionsAPICommand()
        cmd.run(lambda: cmd.retrieve(client, external_id=external_id))

    @staticmethod
    def status() -> None:
        """Get the Cognite Functions activation status for the project

        Shows whether Cognite Functions are inactive, requested or activated. Functions must be activated before any function can be deployed or called.

        \b
        Example:
          cdf api functions status
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = FunctionsAPICommand()
        cmd.run(lambda: cmd.status(client))


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
