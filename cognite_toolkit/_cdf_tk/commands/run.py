from __future__ import annotations

import ast
import datetime
import json
import os
import platform
import re
import shutil
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import questionary
from cognite.client._api.functions import ALLOWED_HANDLE_ARGS
from cognite.client.credentials import OAuthClientCredentials, OAuthInteractive, Token
from cognite.client.data_classes import ClientCredentials, WorkflowTriggerUpsert
from cognite.client.data_classes.transformations import TransformationList
from cognite.client.data_classes.transformations.common import NonceCredentials
from cognite.client.data_classes.workflows import (
    FunctionTaskParameters,
    WorkflowExecutionDetailed,
    WorkflowVersionId,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import ms_to_datetime
from rich import print
from rich.progress import Progress
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.functions import FunctionScheduleID
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes import BuiltResourceFull, ModuleResources
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ToolkitFileNotFoundError,
    ToolkitInvalidFunctionError,
    ToolkitMissingResourceError,
    ToolkitNotADirectoryError,
    ToolkitNotSupported,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.hints import verify_module_directory
from cognite_toolkit._cdf_tk.loaders import FunctionLoader, FunctionScheduleLoader, WorkflowVersionLoader
from cognite_toolkit._cdf_tk.loaders._resource_loaders.workflow_loaders import WorkflowTriggerLoader
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import in_dict
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME, EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import safe_read, safe_rmtree, safe_write

from ._base import ToolkitCommand


@dataclass
class FunctionCallArgs:
    data: dict[str, Any] = field(default_factory=dict)
    authentication: ClientCredentials | None = None
    client_id_env_name: str | None = None
    client_secret_env_name: str | None = None


class RunFunctionCommand(ToolkitCommand):
    virtual_env_folder = "function_local_venvs"
    default_readme_md = """# Local Function Quality Assurance

This directory contains virtual environments for running functions locally. This is
intended to test the function before deploying it to CDF or to debug issues with a deployed function.

"""
    import_check_py = """import sys
from pathlib import Path

# This is necessary to import adjacent modules in the function code.
sys.path.insert(0, str(Path(__file__).parent / "local_code"))

from local_code.{handler_import} import handle # noqa: E402


def main() -> None:
    print("Imported function successfully: " + handle.__name__)


if __name__ == "__main__":
    main()

"""
    run_check_py = """import os
import sys

from pathlib import Path
from pprint import pprint

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import {credentials_cls}

# This is necessary to import adjacent modules in the function code.
sys.path.insert(0, str(Path(__file__).parent / "local_code"))

from local_code.{handler_import} import handle # noqa: E402
{load_dotenv}

def main() -> None:
    credentials = {credentials_cls}(
        {credentials_args}
    )

    client = CogniteClient(
        config=ClientConfig(
            client_name="{client_name}",
            project="{project}",
            base_url="{base_url}",
            credentials=credentials,
        )
    )

    print("{function_external_id} LOGS:")
    response = handle(
        {handler_args}
    )

    print("{function_external_id} RESPONSE:")
    pprint(response)


if __name__ == "__main__":
    main()
"""

    def run_cdf(
        self,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        build_env_name: str | None,
        external_id: str | None = None,
        data_source: str | WorkflowVersionId | None = None,
        wait: bool = False,
    ) -> bool:
        if organization_dir in {Path("."), Path("./")}:
            organization_dir = Path.cwd()
        verify_module_directory(organization_dir, build_env_name)

        resources = ModuleResources(organization_dir, build_env_name)
        is_interactive = external_id is None
        external_id = self._get_function(external_id, resources).identifier
        call_args = self._get_call_args(data_source, external_id, resources, env_vars.dump(), is_interactive)
        client = env_vars.get_client()
        function = client.functions.retrieve(external_id=external_id)
        if function is None:
            raise ToolkitMissingResourceError(
                f"Could not find function with external id {external_id}. Have you deployed it?"
            )

        if is_interactive:
            wait = questionary.confirm("Do you want to wait for the function to complete?").ask()

        # Todo: Get one shot token using the call_args.authentication
        session = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        result = client.functions.call(external_id=external_id, data=call_args.data, wait=False, nonce=session.nonce)

        table = Table(title=f"Function {external_id!r}, id {function.id!r}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Call id", str(result.id))
        table.add_row("Status", str(result.status))
        table.add_row("Created time", str(ms_to_datetime(result.start_time)))
        print(table)

        if not wait:
            return True

        max_time = client.functions.limits().timeout_minutes * 60
        with Progress() as progress:
            call_task = progress.add_task("Waiting for function call to complete...", total=max_time)
            start_time = time.time()
            duration = 0.0
            sleep_time = 1
            while result.status.casefold() == "running" and duration < max_time:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, 60)
                result.update()
                duration = time.time() - start_time
                progress.advance(call_task, advance=duration)
            progress.advance(call_task, advance=max_time - duration)
            progress.stop()
        table = Table(title=f"Function {external_id}, id {function.id}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Call id", str(result.id))
        table.add_row("Status", str(result.status))
        created_time = ms_to_datetime(result.start_time)
        finished_time = ms_to_datetime(result.end_time or (datetime.datetime.now().timestamp() * 1000))
        table.add_row("Created time", str(created_time))
        table.add_row("Finished time", str(finished_time))
        run_time = finished_time - created_time
        table.add_row("Duration", f"{run_time.total_seconds():,} seconds")
        if result.error is not None:
            table.add_row("Error", str(result.error.get("message", "Empty error")))
            table.add_row("Error trace", str(result.error.get("trace", "Empty trace")))
        response = client.functions.calls.get_response(call_id=result.id or 0, function_id=function.id)
        table.add_row("Result", str(json.dumps(response, indent=2, sort_keys=True)))
        logs = client.functions.calls.get_logs(call_id=result.id or 0, function_id=function.id)
        table.add_row("Logs", str(logs))
        print(table)
        return True

    @staticmethod
    def _get_function(external_id: str | None, resources: ModuleResources) -> BuiltResourceFull[str]:
        function_builds_by_identifier = {
            build.identifier: build for build in resources.list_resources(str, "functions", FunctionLoader.kind)
        }

        if len(function_builds_by_identifier) == 0:
            raise ToolkitMissingResourceError(
                "No functions found in modules. Suggest running `cdf modules list` to verify."
            )

        if external_id is None:
            # Interactive mode
            external_id = questionary.select(
                "Select function to run", choices=list(function_builds_by_identifier.keys())
            ).ask()
        elif external_id not in function_builds_by_identifier.keys():
            raise ToolkitMissingResourceError(f"Could not find function with external id {external_id}")
        return function_builds_by_identifier[external_id]

    @classmethod
    def _get_call_args(
        cls,
        data_source: str | WorkflowVersionId | None,
        function_external_id: str,
        resources: ModuleResources,
        environment_variables: dict[str, str | None],
        is_interactive: bool,
    ) -> FunctionCallArgs:
        if data_source is None and (
            not is_interactive or not questionary.confirm("Do you want to provide input data for the function?").ask()
        ):
            return FunctionCallArgs()
        if is_interactive:
            data, credentials = cls._get_call_args_interactive(function_external_id, resources)
        elif data_source is not None:
            data, credentials = cls._geta_call_args_from_data_source(data_source, function_external_id, resources)
        else:
            raise ToolkitValueError("Data source is required when not in interactive mode.")

        if credentials is None:
            return FunctionCallArgs(data)

        if cls._is_environ_var(credentials.client_id):
            env_var = cls._clean_environ_var(credentials.client_id)
            if not (client_id_value := environment_variables.get(env_var)):
                raise ToolkitValueError(f"Missing environment variable {env_var} for function client ID")
            client_id_name = env_var
        else:
            client_id_name = None
            client_id_value = credentials.client_id

        if cls._is_environ_var(credentials.client_secret):
            env_var = cls._clean_environ_var(credentials.client_secret)
            if not (client_secret_value := environment_variables.get(env_var)):
                raise ToolkitValueError(f"Missing environment variable {env_var} for function client secret")
            client_secret_name = env_var
        else:
            client_secret_name = None
            client_secret_value = credentials.client_secret

        return FunctionCallArgs(
            data, ClientCredentials(client_id_value, client_secret_value), client_id_name, client_secret_name
        )

    @staticmethod
    def _is_environ_var(value: str) -> bool:
        return value.startswith("${") and value.endswith("}")

    @staticmethod
    def _clean_environ_var(value: str) -> str:
        return value.removeprefix("${").removesuffix("}")

    @staticmethod
    def _get_call_args_interactive(
        function_external_id: str, resources: ModuleResources
    ) -> tuple[dict[str, Any], ClientCredentials | None]:
        schedules = resources.list_resources(FunctionScheduleID, "functions", FunctionScheduleLoader.kind)
        options: dict[str, Any] = {
            f"FunctionSchedule: {schedule.identifier.name}": schedule
            for schedule in schedules
            if schedule.identifier.function_external_id == function_external_id
        }
        workflows = resources.list_resources(WorkflowVersionId, "workflows", WorkflowVersionLoader.kind)
        raw_trigger_by_workflow_id: dict[WorkflowVersionId, dict[str, Any]] = {}
        for trigger in resources.list_resources(str, "workflows", WorkflowTriggerLoader.kind):
            raw_trigger = trigger.load_resource_dict({}, validate=False)
            loaded_trigger = WorkflowTriggerUpsert.load(raw_trigger)
            raw_trigger_by_workflow_id[
                WorkflowVersionId(loaded_trigger.workflow_external_id, loaded_trigger.workflow_version)
            ] = raw_trigger

        for workflow in workflows:
            raw_workflow = workflow.load_resource_dict({}, validate=False)
            loaded = WorkflowVersionUpsert.load(raw_workflow)
            for task in loaded.workflow_definition.tasks:
                if (
                    isinstance(task.parameters, FunctionTaskParameters)
                    and task.parameters.external_id == function_external_id
                ):
                    data = task.parameters.data if isinstance(task.parameters.data, dict) else {}
                    raw_trigger = raw_trigger_by_workflow_id.get(workflow.identifier, {})
                    options[f"Workflow: {workflow.identifier.workflow_external_id}"] = (
                        data,
                        raw_trigger.get("authentication"),
                    )

        if len(options) == 0:
            print(f"No schedules or workflows found for this {function_external_id} function.")
            return {}, None
        selected_name: str = questionary.select("Select schedule to run", choices=options).ask()  # type: ignore[arg-type]
        selected = options[selected_name]
        if isinstance(selected, BuiltResourceFull):
            # Schedule
            raw_schedule = selected.load_resource_dict({}, validate=False)
            return raw_schedule.get("data", {}), ClientCredentials.load(
                raw_schedule["authentication"]
            ) if "authentication" in raw_schedule else None
        elif (
            isinstance(selected, tuple)
            and len(selected) == 2
            and isinstance(selected[0], dict)
            and isinstance(selected[1], dict)
        ):
            return selected[0], ClientCredentials.load(selected[1]) if in_dict(
                ["clientId", "clientSecret"], selected[1]
            ) else None
        else:
            raise ToolkitValueError(f"Selected value {selected} is not a valid schedule or workflow.")

    @staticmethod
    def _geta_call_args_from_data_source(
        data_source: str | WorkflowVersionId, function_external_id: str, resources: ModuleResources
    ) -> tuple[dict[str, Any], ClientCredentials | None]:
        data: dict[str, Any] | None = None
        credentials: ClientCredentials | None = None
        workflows = resources.list_resources(WorkflowVersionId, "workflows", WorkflowVersionLoader.kind)
        found = False
        for workflow in workflows:
            if (isinstance(data_source, str) and workflow.identifier.workflow_external_id == data_source) or (
                isinstance(data_source, WorkflowVersionId) and workflow.identifier == data_source
            ):
                raw_workflow = workflow.load_resource_dict({}, validate=False)
                loaded = WorkflowVersionUpsert.load(raw_workflow)
                for task in loaded.workflow_definition.tasks:
                    if (
                        isinstance(task.parameters, FunctionTaskParameters)
                        and task.parameters.external_id == function_external_id
                    ):
                        data = task.parameters.data if isinstance(task.parameters.data, dict) else {}
                        found = True
                        break
            for trigger in resources.list_resources(str, "workflows", WorkflowTriggerLoader.kind):
                raw_trigger = trigger.load_resource_dict({}, validate=False)
                loaded_trigger = WorkflowTriggerUpsert.load(raw_trigger)
                if (isinstance(data_source, str) and loaded_trigger.workflow_external_id == data_source) or (
                    isinstance(data_source, WorkflowVersionId)
                    and WorkflowVersionId(loaded_trigger.workflow_external_id, loaded_trigger.workflow_version)
                    == data_source
                ):
                    if "authentication" in raw_trigger:
                        try:
                            credentials = ClientCredentials.load(raw_trigger["authentication"])
                        except KeyError:
                            ...
                        else:
                            found = True
                    break
            if found:
                return data or {}, credentials

        if not isinstance(data_source, str):
            raise ToolkitValueError(f"Data source {data_source} is not a valid workflow external id.")

        for schedule in resources.list_resources(FunctionScheduleID, "functions", FunctionScheduleLoader.kind):
            if (
                schedule.identifier.function_external_id == function_external_id
                and schedule.identifier.name == data_source
            ):
                raw_schedule = schedule.load_resource_dict({}, validate=False)
                return raw_schedule.get("data", {}), ClientCredentials.load(
                    raw_schedule["authentication"]
                ) if "authentication" in raw_schedule else None
        raise ToolkitMissingResourceError(f"Could not find data for source {data_source}")

    def run_local(
        self,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        build_env_name: str | None,
        external_id: str | None = None,
        data_source: str | WorkflowVersionId | None = None,
        rebuild_env: bool = False,
    ) -> None:
        try:
            from ._virtual_env import FunctionVirtualEnvironment
        except ImportError:
            if _RUNNING_IN_BROWSER:
                raise ToolkitNotSupported("This functionality is not supported in a browser environment.")
            raise

        if organization_dir in {Path("."), Path("./")}:
            organization_dir = Path.cwd()
        verify_module_directory(organization_dir, build_env_name)

        resources = ModuleResources(organization_dir, build_env_name)
        function_build = self._get_function(external_id, resources)

        function_external_id = function_build.identifier

        virtual_envs_dir = organization_dir / self.virtual_env_folder
        virtual_envs_dir.mkdir(exist_ok=True)
        readme_overview = virtual_envs_dir / "README.md"
        if not readme_overview.exists():
            safe_write(readme_overview, self.default_readme_md)

        function_venv = Path(virtual_envs_dir) / function_external_id
        function_source_code = function_build.source.path.parent / function_external_id
        if not function_source_code.exists():
            raise ToolkitNotADirectoryError(
                f"Could not find function code for {function_external_id}. Expected at {function_source_code.as_posix()}"
            )

        requirements_txt: Path | str = function_source_code / "requirements.txt"
        if not cast(Path, requirements_txt).exists():
            self.warn(
                MediumSeverityWarning(
                    "No requirements.txt found for function, "
                    "it is recommended that you have one and pin your "
                    "dependencies including the cognite-sdk"
                )
            )

            # Default to install only the SDK in the latest version
            requirements_txt = "cognite-sdk\n"

        print(f"Setting up virtual environment for function {function_external_id}...")
        # Todo: Ensure cognite-sdk is installed in the virtual environment?
        virtual_env = FunctionVirtualEnvironment(requirements_txt, rebuild_env)

        virtual_env.create(function_venv / ".venv")

        print(
            f"    [green]✓ 1/4[/green] Function {function_external_id!r} virtual environment setup complete: "
            f"'requirements.txt' file is valid."
        )

        function_destination_code = function_venv / "local_code"
        if function_destination_code.exists():
            safe_rmtree(function_destination_code)
        shutil.copytree(function_source_code, function_destination_code)
        init_py = function_destination_code / "__init__.py"
        if not init_py.exists():
            # We need a __init__ to avoid import errors when running the function code.
            init_py.touch()

        function_dict = function_build.load_resource_dict(env_vars.dump(), validate=False)
        handler_file = function_dict.get("functionPath", "handler.py")
        handler_path = function_destination_code / handler_file
        if not handler_path.exists():
            raise ToolkitFileNotFoundError("Could not find handler.py file for function.")

        args = self._get_function_args(handler_path, function_name="handle")
        expected = ALLOWED_HANDLE_ARGS
        if not args <= ALLOWED_HANDLE_ARGS:
            raise ToolkitInvalidFunctionError(
                f"Function handle function should have arguments {expected}, got {list(args)}"
            )
        print(
            f"    [green]✓ 2/4[/green] Function {function_external_id!r} the {handler_file!r} "
            f"is valid with arguments {list(args)}."
        )

        import_check = "import_check.py"
        safe_write(
            function_venv / import_check,
            self.import_check_py.format(handler_import=self._create_handler_import(handler_file)),
        )

        virtual_env.execute(Path(import_check), f"import_check {function_external_id}")

        print(f"    [green]✓ 3/4[/green] Function {function_external_id!r} successfully imported code.")

        is_interactive = external_id is None
        call_args = self._get_call_args(
            data_source,
            function_build.identifier,
            resources,
            env_vars.dump(),
            is_interactive,
        )

        run_check_py, env = self._create_run_check_file_with_env(
            env_vars,
            args,
            function_dict,
            function_external_id,
            handler_file,
            call_args,
            function_venv,
        )
        if platform.system() == "Windows":
            if system_root := os.environ.get("SYSTEMROOT"):
                # This is necessary to run python on Windows.
                # https://stackoverflow.com/questions/78652758/cryptic-oserror-winerror-10106-the-requested-service-provider-could-not-be-l
                env["SYSTEMROOT"] = system_root
            # In addition, we need to convert all values to strings.
            env = {k: str(v) for k, v in env.items()}

        run_check = "run_check.py"
        safe_write(function_venv / run_check, run_check_py)
        virtual_env.execute(Path(run_check), f"run_check {function_external_id}", env)

        print(f"    [green]✓ 4/4[/green] Function {function_external_id!r} successfully executed code.")

    def _create_run_check_file_with_env(
        self,
        env_vars: EnvironmentVariables,
        args: set[str],
        function_dict: dict[str, Any],
        function_external_id: str,
        handler_file: str,
        call_args: FunctionCallArgs,
        function_venv_dir: Path,
    ) -> tuple[str, dict[str, str]]:
        if authentication := call_args.authentication:
            if authentication.client_id.startswith("${"):
                raise ToolkitInvalidFunctionError(
                    f"Missing environment variable for clientId in schedule authentication, {authentication.client_id}"
                )
            if authentication.client_secret.startswith("${"):
                raise ToolkitInvalidFunctionError(
                    f"Missing environment variable for clientSecret in schedule authentication, {authentication.client_secret}"
                )
            print(f"Using schedule authentication to run {function_external_id!r}.")
            secret_env_name = call_args.client_secret_env_name or "IDP_CLIENT_SECRET"
            credentials_args = {
                "token_url": f'"{env_vars.idp_token_url}"',
                "client_id": f'"{authentication.client_id}"',
                "client_secret": f'os.environ["{secret_env_name}"]',
                "scopes": str(env_vars.idp_scopes),
            }
            env = {
                secret_env_name: authentication.client_secret,
            }
            credentials_cls = OAuthClientCredentials.__name__
        else:
            print(f"Using Toolkit authentication to run {function_external_id!r}.")
            env = {}
            if env_vars.LOGIN_FLOW == "token":
                credentials_cls = Token.__name__
                credentials_args = {"token": 'os.environ["CDF_TOKEN"]'}
                env["CDF_TOKEN"] = env_vars.CDF_TOKEN  # type: ignore[assignment]
            elif env_vars.LOGIN_FLOW == "interactive":
                credentials_cls = OAuthInteractive.__name__
                credentials_args = {
                    "authority_url": f'"{env_vars.idp_authority_url}"',
                    "client_id": f'"{env_vars.IDP_CLIENT_ID}"',
                    "scopes": str(env_vars.idp_scopes),
                }
            elif env_vars.LOGIN_FLOW == "client_credentials":
                credentials_cls = OAuthClientCredentials.__name__
                credentials_args = {
                    "token_url": f'"{env_vars.idp_token_url}"',
                    "client_id": f'"{env_vars.IDP_CLIENT_ID}"',
                    "client_secret": 'os.environ["IDP_CLIENT_SECRET"]',
                    "scopes": str(env_vars.idp_scopes),
                }
                env["IDP_CLIENT_SECRET"] = env_vars.IDP_CLIENT_SECRET  # type: ignore[assignment]
            else:
                raise ToolkitNotSupported(f"Login flow {env_vars.LOGIN_FLOW} is not supported .")

        handler_args: dict[str, Any] = {}
        if "client" in args:
            handler_args["client"] = "client"
        if "data" in args:
            handler_args["data"] = str(call_args.data)
        if "secrets" in args:
            handler_args["secrets"] = str(function_dict.get("secrets", {}))
        if "function_call_info" in args:
            handler_args["function_call_info"] = str({"local": True})

        load_dotenv = ""
        if (Path.cwd() / ".env").is_file():
            load_dotenv = textwrap.dedent("""
                try:
                    from dotenv import load_dotenv

                    for parent in Path(__file__).resolve().parents:
                        if (parent / ".env").exists():
                            load_dotenv(parent / '.env')
                except ImportError:
                    ...
            """)

        handler_import = self._create_handler_import(handler_file)

        run_check_py = self.run_check_py.format(
            credentials_cls=credentials_cls,
            handler_import=handler_import,
            client_name=CLIENT_NAME,
            project=env_vars.CDF_PROJECT,
            base_url=env_vars.cdf_url,
            credentials_args="\n        ".join(f"{k}={v}," for k, v in credentials_args.items()),
            handler_args="\n        ".join(f"{k}={v}," for k, v in handler_args.items()),
            function_external_id=function_external_id,
            load_dotenv=load_dotenv,
        )
        return run_check_py, env

    @staticmethod
    def _get_function_args(py_file: Path, function_name: str) -> set[str]:
        parsed = ast.parse(safe_read(py_file))
        handle_function = next(
            (item for item in parsed.body if isinstance(item, ast.FunctionDef) and item.name == function_name), None
        )
        if handle_function is None:
            raise ToolkitInvalidFunctionError(f"No {function_name} function found in {py_file}")
        return {a.arg for a in handle_function.args.args}

    @staticmethod
    def _create_handler_import(handler_file: str) -> str:
        return re.sub(r"\.+", ".", handler_file.replace(".py", "").replace("/", ".")).removeprefix(".")


class RunTransformationCommand(ToolkitCommand):
    def run_transformation(self, client: ToolkitClient, external_ids: str | list[str]) -> bool:
        """Run a transformation in CDF"""
        if isinstance(external_ids, str):
            external_ids = [external_ids]
        try:
            transformations: TransformationList = client.transformations.retrieve_multiple(external_ids=external_ids)
        except CogniteAPIError as e:
            print("[bold red]ERROR:[/] Could not retrieve transformations.")
            print(e)
            return False
        if transformations is None or len(transformations) == 0:
            print(f"[bold red]ERROR:[/] Could not find transformation with external_id {external_ids}")
            return False
        for transformation in transformations:
            session = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
            if session is None:
                print("[bold red]ERROR:[/] Could not get a oneshot session.")
                return False
            nonce = NonceCredentials(session_id=session.id, nonce=session.nonce, cdf_project_name=client.config.project)
            transformation.source_nonce = nonce
            transformation.destination_nonce = nonce
        try:
            client.transformations.update(transformations)
        except CogniteAPIError as e:
            print("[bold red]ERROR:[/] Could not update transformations with oneshot session.")
            print(e)
            return False
        for transformation in transformations:
            try:
                job = client.transformations.run(transformation_external_id=transformation.external_id, wait=False)
                print(f"Running transformation {transformation.external_id}, status {job.status}...")
            except CogniteAPIError as e:
                print(f"[bold red]ERROR:[/] Could not run transformation {transformation.external_id}.")
                print(e)
        return True


class RunWorkflowCommand(ToolkitCommand):
    def run_workflow(
        self,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        build_env_name: str | None,
        external_id: str | None,
        version: str | None,
        wait: bool,
    ) -> bool:
        """Run a workflow in CDF"""
        resources = ModuleResources(organization_dir, build_env_name)
        client = env_vars.get_client()
        is_interactive = external_id is None
        workflows = resources.list_resources(WorkflowVersionId, "workflows", WorkflowVersionLoader.kind)
        if len(workflows) == 0:
            raise ToolkitMissingResourceError("No workflows found in modules.")
        if external_id is None:
            # Interactive mode
            choices = [questionary.Choice(title=f"{build.identifier!r}", value=build) for build in workflows]
            selected = questionary.select("Select workflow to run", choices=choices).ask()
        else:
            selected_ = next(
                (
                    build
                    for build in workflows
                    if build.identifier.workflow_external_id == external_id
                    and (version is None or (build.identifier.version == version))
                ),
                None,
            )
            if selected_ is None:
                raise ToolkitMissingResourceError(f"Could not find workflow with external id {external_id}")
            selected = selected_
        id_ = selected.identifier
        triggers = resources.list_resources(str, "workflows", WorkflowTriggerLoader.kind)

        credentials: ClientCredentials | None = None
        input_: dict | None = None
        for trigger in triggers:
            trigger_dict = trigger.load_resource_dict(env_vars.dump(), validate=False)
            if (
                trigger_dict["workflowExternalId"] == id_.workflow_external_id
                and trigger_dict["workflowVersion"] == id_.version
            ):
                print(f"Found trigger {trigger.identifier!r} for workflow {id_!r}")
                if (
                    is_interactive
                    and not questionary.confirm(
                        "Do you want to use input data and authentication from this trigger?"
                    ).ask()
                ):
                    break
                credentials = (
                    ClientCredentials.load(trigger_dict["authentication"]) if "authentication" in trigger_dict else None
                )
                input_ = trigger_dict.get("input")
                break

        try:
            if credentials:
                client = ToolkitClient(
                    config=ToolkitClientConfig(
                        client_name=CLIENT_NAME,
                        project=env_vars.CDF_PROJECT,
                        base_url=env_vars.cdf_url,
                        credentials=OAuthClientCredentials(
                            token_url=env_vars.idp_token_url,
                            client_id=credentials.client_id,
                            client_secret=credentials.client_secret,
                            scopes=env_vars.idp_scopes,
                        ),
                    )
                )
                nonce = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE").nonce
            else:
                nonce = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE").nonce
        except CogniteAPIError as e:
            raise AuthorizationError(f"Could not create oneshot session for workflow {id_!r}: {e!s}") from e

        if is_interactive:
            wait = questionary.confirm("Do you want to wait for the workflow to complete?").ask()
        if id_.version is None:
            raise ToolkitValueError("Version is required for workflow.")
        execution = client.workflows.executions.run(
            workflow_external_id=id_.workflow_external_id,
            version=id_.version,
            input=input_,
            nonce=nonce,
        )
        table = Table(title=f"Workflow {id_!r}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Execution id", str(execution.id))
        table.add_row("Status", str(execution.status))
        table.add_row("Created time", f"{ms_to_datetime(execution.start_time or 0):%Y-%m-%d %H:%M:%S}")
        print(table)

        if not wait:
            return True

        workflow = client.workflows.versions.retrieve(id_)
        if workflow is None:
            raise ToolkitMissingResourceError(f"Could not find workflow {id_!r}")

        total = len(workflow.workflow_definition.tasks)
        max_time = sum((task.timeout or 3600) * (task.retries or 3) for task in workflow.workflow_definition.tasks)
        result = cast(WorkflowExecutionDetailed, client.workflows.executions.retrieve_detailed(execution.id))
        with Progress() as progress:
            call_task = progress.add_task("Waiting for workflow execution to complete...", total=total)
            start_time = time.time()
            duration = 0.0
            sleep_time = 1
            while (result is None or result.status.upper() == "RUNNING") and duration < max_time:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, 15)
                result = cast(
                    WorkflowExecutionDetailed,
                    client.workflows.executions.retrieve_detailed(execution.id),
                )
                duration = time.time() - start_time
                completed_count = sum(
                    1 for task in result.executed_tasks if task.status.upper() not in {"IN_PROGRESS", "SCHEDULED"}
                )
                # Todo remove this print statement. It is added to check why the progress goes early to 100%.
                task_statuses = {task.external_id: task.status for task in result.executed_tasks}
                print(f"Status: {task_statuses}")
                print(f"Complete count: {completed_count}, total: {total}")
                progress.advance(call_task, advance=completed_count)
            progress.advance(call_task, advance=total)
            progress.stop()
        if result is None:
            print(f"Could not find execution {execution.id}")
            return False

        print(f"Workflow {id_!r} execution {execution.id} completed with status {result.status}")

        table = Table(title=f"Workflow Tasks {id_!r}")
        table.add_column("Task")
        table.add_column("Status")
        table.add_column("Start Time")
        table.add_column("End Time")
        table.add_column("Duration")
        table.add_column("ReasonForIncompletion")

        for task in result.executed_tasks:
            task_duration = (
                f"{datetime.timedelta(seconds=(task.end_time - task.start_time) / 1000).total_seconds():.1f} seconds"
                if task.end_time and task.start_time
                else ""
            )
            table.add_row(
                task.external_id,
                task.status,
                f"{ms_to_datetime(task.start_time):%Y-%m-%d %H:%M:%S}" if task.start_time else "",
                f"{ms_to_datetime(task.end_time):%Y-%m-%d %H:%M:%S}" if task.end_time else "",
                task_duration,
                task.reason_for_incompletion,
            )
        print(table)
        return True
