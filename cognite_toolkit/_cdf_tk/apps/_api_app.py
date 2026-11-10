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


class FunctionStatusEnum(StrEnum):
    queued = "Queued"
    deploying = "Deploying"
    ready = "Ready"
    failed = "Failed"
    retired = "Retired"


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
        self.command()(self.list)
        self.command()(self.retrieve)
        self.command()(self.run)
        self.command()(self.status)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with Cognite Functions

        Calls the Cognite Functions API directly, without building or deploying any local modules. Authentication is read from the environment, the same way as for the other `cdf` commands.

        \b
        Examples:
          cdf api functions list
          cdf api functions retrieve --external-id my_function
          cdf api functions run --external-id my_function --json '{"key": "value"}'
          cdf api functions status
        """
        print_help_if_no_subcommand(ctx)

    @staticmethod
    def list(
        limit: int = typer.Option(
            DEFAULT_LIST_LIMIT,
            "--limit",
            "-l",
            help="Maximum number of functions to return.",
            min=1,
            max=100,
        ),
        status: FunctionStatusEnum | None = typer.Option(
            None,
            "--status",
            "-s",
            help="Only list functions with this deployment status.",
            case_sensitive=False,
        ),
        owner: str | None = typer.Option(
            None,
            "--owner",
            "-o",
            help="Only list functions with this owner (exact match).",
        ),
    ) -> None:
        """List functions, including their deployment status

        Prints the functions in the project as returned by CDF. Each function has a `status` field, which is one of Queued, Deploying, Ready, Failed or Retired. Use `retrieve` to look at a single function.

        The Functions API cannot filter, so --status and --owner are applied after the functions are fetched. --limit then applies to the matching functions.

        \b
        Examples:
          List the first 25 functions:
          cdf api functions list

        \b
          List up to 100 functions:
          cdf api functions list --limit 100

        \b
          List functions that failed to deploy:
          cdf api functions list --status failed

        \b
          List the functions of one owner:
          cdf api functions list --owner "Jane Doe"
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = FunctionsAPICommand()
        cmd.run(lambda: cmd.list(client, limit=limit, status=status.value if status else None, owner=owner))

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
    def run(
        external_id: str = typer.Option(
            ...,
            "--external-id",
            "-e",
            help="External ID of the function to run.",
        ),
        json: str | None = typer.Option(
            None,
            "--json",
            "-j",
            help="A JSON object passed to the function as its input data (the 'data' argument), "
            'for example \'{"key": "value"}\'. YAML is also accepted.',
        ),
        wait: bool = typer.Option(
            True,
            "--wait/--no-wait",
            help="Wait for the call to finish and print its result. With --no-wait, only the created call is printed.",
        ),
        timeout: int | None = typer.Option(
            None,
            "--timeout",
            "-t",
            help="Maximum number of seconds to wait for the call to finish. "
            "Defaults to the function timeout of the CDF project. Cannot be used with --no-wait.",
            min=1,
        ),
    ) -> None:
        """Call a function and wait for the result, optionally with a JSON payload

        The function is called with a one-shot session token for the current credentials. The payload given with --json is passed to the function as its `data` argument, so do not put secrets in it.

        By default, the command waits until the call finishes, or until the timeout is reached (the function timeout of the project, unless --timeout is given). It prints the call, the value returned by the function and the logs. The command fails if the call does not complete successfully.

        With --no-wait, the command returns right after the call is created and prints the call, including its `id` and `status`. --no-wait cannot be combined with --timeout.

        \b
        Examples:
          Call a function without input:
          cdf api functions run --external-id my_function

        \b
          Call a function with a JSON payload:
          cdf api functions run -e my_function --json '{"asset_ids": [1, 2, 3], "dry_run": true}'

        \b
          Payload from a file:
          cdf api functions run -e my_function --json "$(cat payload.json)"

        \b
          Wait at most 60 seconds for the result:
          cdf api functions run -e my_function --timeout 60

        \b
          Start the call and return immediately:
          cdf api functions run -e my_function --no-wait
        """
        if not wait and timeout is not None:
            # Fail before the client is created, so the flags can be checked without credentials.
            raise typer.BadParameter("--timeout cannot be used together with --no-wait.")
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = FunctionsAPICommand()
        cmd.run(lambda: cmd.call(client, external_id=external_id, payload=json, wait=wait, timeout=timeout))

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
