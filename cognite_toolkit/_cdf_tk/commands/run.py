from __future__ import annotations

import ast
import datetime
import json
import os
import platform
import re
import shutil
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
    WorkflowVersionId,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import ms_to_datetime
from rich import print
from rich.progress import Progress
from rich.table import Table

from cognite_toolkit._cdf_tk.client.data_classes.functions import FunctionScheduleID
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes import BuiltResourceFull, ModuleResources
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitInvalidFunctionError,
    ToolkitMissingResourceError,
    ToolkitNotADirectoryError,
    ToolkitNotSupported,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.hints import verify_module_directory
from cognite_toolkit._cdf_tk.loaders import FunctionLoader, FunctionScheduleLoader, WorkflowVersionLoader
from cognite_toolkit._cdf_tk.loaders._resource_loaders.workflow_loaders import WorkflowTriggerLoader
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, get_oneshot_session

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
    import_check_py = """from cognite.client._api.functions import validate_function_folder


def main() -> None:
    validate_function_folder(
        root_path="local_code/",
        function_path="{handler_py}",
        skip_folder_validation=False,
    )

if __name__ == "__main__":
    main()
"""
    run_check_py = """import os
from pprint import pprint

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import {credentials_cls}

from local_code.{handler_import} import handle


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
        ToolGlobals: CDFToolConfig,
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
        call_args = self._get_call_args(
            data_source, external_id, resources, ToolGlobals.environment_variables(), is_interactive
        )

        client = ToolGlobals.toolkit_client
        function = client.functions.retrieve(external_id=external_id)
        if function is None:
            raise ToolkitMissingResourceError(
                f"Could not find function with external id {external_id}. Have you deployed it?"
            )

        if is_interactive:
            wait = questionary.confirm("Do you want to wait for the function to complete?").ask()

        # Todo: Get one shot token using the call_args.authentication
        session = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        result = ToolGlobals.toolkit_client.functions.call(
            external_id=external_id, data=call_args.data, wait=False, nonce=session.nonce
        )

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
        logs = ToolGlobals.toolkit_client.functions.calls.get_logs(call_id=result.id or 0, function_id=function.id)
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
        if Flags.RUN_WORKFLOW.is_enabled():
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
            items = "schedules or workflows" if Flags.RUN_WORKFLOW.is_enabled() else "schedules"
            print(f"No {items} found for this {function_external_id} function.")
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
            return selected[0], ClientCredentials.load(selected[1]["authentication"]) if "authentication" in selected[
                1
            ] else None
        else:
            raise ToolkitValueError(f"Selected value {selected} is not a valid schedule or workflow.")

    @staticmethod
    def _geta_call_args_from_data_source(
        data_source: str | WorkflowVersionId, function_external_id: str, resources: ModuleResources
    ) -> tuple[dict[str, Any], ClientCredentials | None]:
        data: dict[str, Any] | None = None
        credentials: ClientCredentials | None = None
        if Flags.RUN_WORKFLOW.is_enabled():
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
        ToolGlobals: CDFToolConfig,
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
            readme_overview.write_text(self.default_readme_md)

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
            shutil.rmtree(function_destination_code)
        shutil.copytree(function_source_code, function_destination_code)
        init_py = function_destination_code / "__init__.py"
        if not init_py.exists():
            # We need a __init__ to avoid import errors when running the function code.
            init_py.touch()

        function_dict = function_build.load_resource_dict(ToolGlobals.environment_variables(), validate=False)
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
        (function_venv / import_check).write_text(self.import_check_py.format(handler_py=handler_file))

        virtual_env.execute(Path(import_check), f"import_check {function_external_id}")

        print(f"    [green]✓ 3/4[/green] Function {function_external_id!r} successfully imported code.")

        is_interactive = external_id is None
        call_args = self._get_call_args(
            data_source, function_build.identifier, resources, ToolGlobals.environment_variables(), is_interactive
        )

        run_check_py, env = self._create_run_check_file_with_env(
            ToolGlobals, args, function_dict, function_external_id, handler_file, call_args
        )
        if platform.system() == "Windows":
            if system_root := os.environ.get("SYSTEMROOT"):
                # This is necessary to run python on Windows.
                # https://stackoverflow.com/questions/78652758/cryptic-oserror-winerror-10106-the-requested-service-provider-could-not-be-l
                env["SYSTEMROOT"] = system_root
            # In addition, we need to convert all values to strings.
            env = {k: str(v) for k, v in env.items()}

        run_check = "run_check.py"
        (function_venv / run_check).write_text(run_check_py)
        virtual_env.execute(Path(run_check), f"run_check {function_external_id}", env)

        print(f"    [green]✓ 4/4[/green] Function {function_external_id!r} successfully executed code.")

    def _create_run_check_file_with_env(
        self,
        ToolGlobals: CDFToolConfig,
        args: set[str],
        function_dict: dict[str, Any],
        function_external_id: str,
        handler_file: str,
        call_args: FunctionCallArgs,
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
                "token_url": f'"{ToolGlobals._token_url}"',
                "client_id": f'"{authentication.client_id}"',
                "client_secret": f'os.environ["{secret_env_name}"]',
                "scopes": str(ToolGlobals._scopes),
            }
            env = {
                secret_env_name: authentication.client_secret,
            }
            credentials_cls = OAuthClientCredentials.__name__
        else:
            print(f"Using Toolkit authentication to run {function_external_id!r}.")
            env = {}
            if ToolGlobals._login_flow == "token":
                credentials_cls = Token.__name__
                credentials_args = {"token": 'os.environ["CDF_TOKEN"]'}
                env["CDF_TOKEN"] = ToolGlobals._credentials_args["token"]
            elif ToolGlobals._login_flow == "interactive":
                credentials_cls = OAuthInteractive.__name__
                credentials_args = {
                    "authority_url": '"{}"'.format(ToolGlobals._credentials_args["authority_url"]),
                    "client_id": '"{}"'.format(ToolGlobals._credentials_args["client_id"]),
                    "scopes": str(ToolGlobals._scopes),
                }
            elif ToolGlobals._login_flow == "client_credentials":
                credentials_cls = OAuthClientCredentials.__name__
                credentials_args = {
                    "token_url": '"{}"'.format(ToolGlobals._credentials_args["token_url"]),
                    "client_id": '"{}"'.format(ToolGlobals._credentials_args["client_id"]),
                    "client_secret": 'os.environ["IDP_CLIENT_SECRET"]',
                    "scopes": str(ToolGlobals._scopes),
                }
                env["IDP_CLIENT_SECRET"] = ToolGlobals._credentials_args["client_secret"]
            else:
                raise ToolkitNotSupported(f"Login flow {ToolGlobals._login_flow} is not supported .")

        handler_args: dict[str, Any] = {}
        if "client" in args:
            handler_args["client"] = "client"
        if "data" in args:
            handler_args["data"] = str(call_args.data)
        if "secrets" in args:
            handler_args["secrets"] = str(function_dict.get("secrets", {}))
        if "function_call_info" in args:
            handler_args["function_call_info"] = str({"local": True})

        run_check_py = self.run_check_py.format(
            credentials_cls=credentials_cls,
            handler_import=re.sub(r"\.+", ".", handler_file.replace(".py", "").replace("/", ".")).removeprefix("."),
            client_name=ToolGlobals._client_name,
            project=ToolGlobals._project,
            base_url=ToolGlobals._cdf_url,
            credentials_args="\n        ".join(f"{k}={v}," for k, v in credentials_args.items()),
            handler_args="\n        ".join(f"{k}={v}," for k, v in handler_args.items()),
            function_external_id=function_external_id,
        )
        return run_check_py, env

    @staticmethod
    def _get_function_args(py_file: Path, function_name: str) -> set[str]:
        parsed = ast.parse(py_file.read_text())
        handle_function = next(
            (item for item in parsed.body if isinstance(item, ast.FunctionDef) and item.name == function_name), None
        )
        if handle_function is None:
            raise ToolkitInvalidFunctionError(f"No {function_name} function found in {py_file}")
        return {a.arg for a in handle_function.args.args}


class RunTransformationCommand(ToolkitCommand):
    def run_transformation(self, ToolGlobals: CDFToolConfig, external_ids: str | list[str]) -> bool:
        """Run a transformation in CDF"""
        if isinstance(external_ids, str):
            external_ids = [external_ids]
        session = get_oneshot_session(ToolGlobals.toolkit_client)
        if session is None:
            print("[bold red]ERROR:[/] Could not get a oneshot session.")
            return False
        try:
            transformations: TransformationList = ToolGlobals.toolkit_client.transformations.retrieve_multiple(
                external_ids=external_ids
            )
        except CogniteAPIError as e:
            print("[bold red]ERROR:[/] Could not retrieve transformations.")
            print(e)
            return False
        if transformations is None or len(transformations) == 0:
            print(f"[bold red]ERROR:[/] Could not find transformation with external_id {external_ids}")
            return False
        nonce = NonceCredentials(session_id=session.id, nonce=session.nonce, cdf_project_name=ToolGlobals.project)
        for transformation in transformations:
            transformation.source_nonce = nonce
            transformation.destination_nonce = nonce
        try:
            ToolGlobals.toolkit_client.transformations.update(transformations)
        except CogniteAPIError as e:
            print("[bold red]ERROR:[/] Could not update transformations with oneshot session.")
            print(e)
            return False
        for transformation in transformations:
            try:
                job = ToolGlobals.toolkit_client.transformations.run(
                    transformation_external_id=transformation.external_id, wait=False
                )
                print(f"Running transformation {transformation.external_id}, status {job.status}...")
            except CogniteAPIError as e:
                print(f"[bold red]ERROR:[/] Could not run transformation {transformation.external_id}.")
                print(e)
        return True


class RunWorkflowCommand(ToolkitCommand):
    def run_workflow(
        self,
        ToolGlobals: CDFToolConfig,
        organization_dir: Path,
        build_env_name: str | None,
        external_id: str | None,
        version: str | None,
    ) -> bool:
        """Run a workflow in CDF"""
        raise NotImplementedError("This method is not implemented yet.")
