from __future__ import annotations

import ast
import datetime
import json
import os
import platform
import re
import shutil
import time
from pathlib import Path
from typing import Any, cast

import questionary
from cognite.client._api.functions import ALLOWED_HANDLE_ARGS
from cognite.client.credentials import OAuthClientCredentials, OAuthInteractive, Token
from cognite.client.data_classes.transformations import TransformationList
from cognite.client.data_classes.transformations.common import NonceCredentials
from cognite.client.utils import ms_to_datetime
from rich import print
from rich.progress import Progress
from rich.table import Table

from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes import ModuleResources, ResourceBuildInfoFull
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitInvalidFunctionError,
    ToolkitMissingResourceError,
    ToolkitNotADirectoryError,
    ToolkitNotSupported,
)
from cognite_toolkit._cdf_tk.hints import verify_module_directory
from cognite_toolkit._cdf_tk.loaders import FunctionLoader, FunctionScheduleLoader
from cognite_toolkit._cdf_tk.loaders.data_classes import FunctionScheduleID
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, get_oneshot_session

from ._base import ToolkitCommand


class RunFunctionCommand(ToolkitCommand):
    virtual_env_folder = "function_local_venvs"
    default_readme_md = """# Local Function Quality Assurance

This directory contains virtual environments for running functions locally. This is
intended to test the function before deploying it to CDF or to debug issues with a deployed function.

"""
    import_check_py = """from cognite.client._api.functions import validate_function_folder


def main() -> None:
    validate_function_folder(
        root_path="code/",
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

from code.{handler_import} import handle


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
        schedule: str | None = None,
        wait: bool = False,
    ) -> bool:
        if organization_dir in {Path("."), Path("./")}:
            organization_dir = Path.cwd()
        verify_module_directory(organization_dir, build_env_name)

        resources = ModuleResources(organization_dir, build_env_name)
        is_interactive = external_id is None
        external_id = self._get_function(external_id, resources).identifier
        schedule_dict = self._get_schedule_dict(ToolGlobals, schedule, external_id, resources, is_interactive) or {}
        if "data" not in schedule_dict and schedule_dict:
            raise ToolkitMissingResourceError(f"The schedule {schedule_dict['name']} has no data")
        input_data = schedule_dict.get("data", None)

        client = ToolGlobals.toolkit_client
        function = client.functions.retrieve(external_id=external_id)
        if function is None:
            raise ToolkitMissingResourceError(
                f"Could not find function with external id {external_id}. Have you deployed it?"
            )

        if is_interactive:
            wait = questionary.confirm("Do you want to wait for the function to complete?").ask()

        session = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        result = ToolGlobals.toolkit_client.functions.call(
            external_id=external_id, data=input_data, wait=False, nonce=session.nonce
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
    def _get_function(external_id: str | None, resources: ModuleResources) -> ResourceBuildInfoFull[str]:
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

    @staticmethod
    def _get_schedule_dict(
        ToolGlobals: CDFToolConfig,
        schedule_name: str | None,
        external_id: str,
        resources: ModuleResources,
        is_interactive: bool,
    ) -> dict | None:
        if schedule_name is None and (
            not is_interactive or not questionary.confirm("Do you want to provide input data for the function?").ask()
        ):
            return None
        schedules = resources.list_resources(FunctionScheduleID, "functions", FunctionScheduleLoader.kind)
        if is_interactive:
            # Interactive mode
            options = {
                schedule.identifier.name: schedule
                for schedule in schedules
                if schedule.identifier.function_external_id == external_id
            }
            if len(options) == 0:
                print(f"No schedules found for this {external_id} function.")
                return None
            selected_name: str = questionary.select("Select schedule to run", choices=options).ask()  # type: ignore[arg-type]
            selected = options[selected_name]
        else:
            for schedule in schedules:
                if (
                    schedule.identifier.function_external_id == external_id
                    and schedule.identifier.name == schedule_name
                ):
                    selected = schedule
                    break
            else:
                raise ToolkitMissingResourceError(f"Could not find data for schedule {schedule_name}")
        return selected.load_resource_dict(ToolGlobals.environment_variables(), validate=False)

    def run_local(
        self,
        ToolGlobals: CDFToolConfig,
        organization_dir: Path,
        build_env_name: str | None,
        external_id: str | None = None,
        schedule: str | None = None,
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
        function_source_code = function_build.location.path.parent / function_external_id
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

        function_destination_code = function_venv / "code"
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
        schedule_dict = (
            self._get_schedule_dict(ToolGlobals, schedule, function_build.identifier, resources, is_interactive) or {}
        )
        run_check_py, env = self._create_run_check_file_with_env(
            ToolGlobals, args, function_dict, function_external_id, handler_file, schedule_dict
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
        schedule_dict: dict[str, Any],
    ) -> tuple[str, dict[str, str]]:
        if authentication := schedule_dict.get("authentication"):
            if "clientId" not in authentication or "clientSecret" not in authentication:
                raise ToolkitInvalidFunctionError(
                    "Authentication data for schedule should contain 'clientId' and 'clientSecret'"
                )
            if authentication["clientId"].startswith("${"):
                raise ToolkitInvalidFunctionError(
                    f"Missing environment variable for clientId in schedule authentication, {authentication['clientId']}"
                )
            if authentication["clientSecret"].startswith("${"):
                raise ToolkitInvalidFunctionError(
                    f"Missing environment variable for clientSecret in schedule authentication, {authentication['clientSecret']}"
                )
            print(f"Using schedule authentication to run {function_external_id!r}.")
            credentials_args = {
                "token_url": f'"{ToolGlobals._token_url}"',
                "client_id": '"{}"'.format(authentication["clientId"]),
                "client_secret": 'os.environ["IDP_CLIENT_SECRET"]',
                "scopes": str(ToolGlobals._scopes),
            }
            env = {
                "IDP_CLIENT_SECRET": authentication["clientSecret"],
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
            handler_args["data"] = str(schedule_dict.get("data", {}))
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
        except Exception as e:
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
        except Exception as e:
            print("[bold red]ERROR:[/] Could not update transformations with oneshot session.")
            print(e)
            return False
        for transformation in transformations:
            try:
                job = ToolGlobals.toolkit_client.transformations.run(
                    transformation_external_id=transformation.external_id, wait=False
                )
                print(f"Running transformation {transformation.external_id}, status {job.status}...")
            except Exception as e:
                print(f"[bold red]ERROR:[/] Could not run transformation {transformation.external_id}.")
                print(e)
        return True
