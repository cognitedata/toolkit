from __future__ import annotations

import datetime
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from cognite.client.data_classes import FunctionCall, FunctionScheduleWriteList, FunctionWriteList
from cognite.client.data_classes.transformations import TransformationList
from cognite.client.data_classes.transformations.common import NonceCredentials
from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.constants import _RUNNING_IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, SystemYAML
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.loaders import FunctionLoader, FunctionScheduleLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, get_oneshot_session, module_from_path

from ._base import ToolkitCommand


class RunFunctionCommand(ToolkitCommand):
    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        external_id: str,
        payload: str | None,
        follow: bool,
        local: bool,
        rebuild_env: bool,
        no_cleanup: bool,
        source_dir: str | None,
        schedule: str | None,
        build_env_name: str,
        verbose: bool,
    ) -> None:
        if not local:
            self.run_function(ToolGlobals, external_id=external_id, payload=payload or "", follow=follow)
            return None
        if follow:
            print(
                "  [bold yellow]WARNING:[/] --follow is not supported when running locally and should not be specified."
            )
        if source_dir is None:
            source_dir = "./"
        source_path = Path(source_dir)
        if not source_path.exists():
            raise ToolkitFileNotFoundError(f"Could not find source directory {source_path}")
        _ = SystemYAML.load_from_directory(source_path, build_env_name)

        self.run_local_function(
            ToolGlobals=ToolGlobals,
            source_path=source_path,
            external_id=external_id,
            payload=payload or "{}",
            schedule=schedule,
            build_env_name=build_env_name,
            rebuild_env=rebuild_env,
            verbose=verbose,
            no_cleanup=no_cleanup,
        )

    def run_function(self, ToolGlobals: CDFToolConfig, external_id: str, payload: str, follow: bool = False) -> bool:
        """Run a function in CDF"""
        session = get_oneshot_session(ToolGlobals.client)
        if session is None:
            print("[bold red]ERROR:[/] Could not get a oneshot session.")
            return False
        try:
            function = ToolGlobals.client.functions.retrieve(external_id=external_id)
        except Exception as e:
            print("[bold red]ERROR:[/] Could not retrieve function.")
            print(e)
            return False
        if function is None:
            print(f"[bold red]ERROR:[/] Could not find function with external_id {external_id}")
            return False
        try:
            data: dict[str, Any] = json.loads(payload or "{}")
        except Exception as e:
            print("[bold red]ERROR:[/] Could not parse payload.")
            print(e)
            return False

        def _function_call(id: int, payload: dict[str, Any]) -> FunctionCall | None:
            (_, bearer) = ToolGlobals.client.config.credentials.authorization_header()
            session = get_oneshot_session(ToolGlobals.client)
            if session is None:
                print("[bold red]ERROR:[/] Could not get a oneshot session.")
                return None
            nonce = session.nonce
            ret = ToolGlobals.client.post(
                url=f"/api/v1/projects/{ToolGlobals.project}/functions/{id}/call",
                json={
                    "data": payload,
                    "nonce": nonce,
                },
                headers={"Authorization": bearer},
            )
            if ret.status_code == 201:
                return FunctionCall.load(ret.json())
            return None

        try:
            call_result = _function_call(id=function.id, payload=data)
            if call_result is None:
                print("[bold red]ERROR:[/] Could not run function.")
                return False
        except Exception as e:
            print("[bold red]ERROR:[/] Could not run function.")
            print(e)
            return False
        table = Table(title=f"Function {external_id}, id {function.id}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Call id", str(call_result.id))
        table.add_row("Status", str(call_result.status))
        table.add_row("Created time", str(datetime.datetime.fromtimestamp((call_result.start_time or 1000) / 1000)))
        print(table)

        if follow:
            print("Awaiting results from function call...")
            sleep_time = 1
            total_time = 0
            while True and total_time < 540:  # 9 minutes timeout in Azure
                total_time += sleep_time
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, 60)
                call_result = ToolGlobals.client.functions.calls.retrieve(
                    call_id=call_result.id or 0, function_id=function.id
                )
                if call_result is None:
                    print("[bold red]ERROR:[/] Could not retrieve function call result.")
                    return False
                if call_result.status != "Running":
                    break
            table = Table(title=f"Function {external_id}, id {function.id}")
            table.add_column("Info", justify="left")
            table.add_column("Value", justify="left", style="green")
            table.add_row("Call id", str(call_result.id))
            table.add_row("Status", str(call_result.status))
            table.add_row("Created time", str(datetime.datetime.fromtimestamp((call_result.start_time or 1000) / 1000)))
            table.add_row("Finished time", str(datetime.datetime.fromtimestamp((call_result.end_time or 1000) / 1000)))
            table.add_row("Duration", str((call_result.end_time or 1) - (call_result.start_time or 1)))
            if call_result.error is not None:
                table.add_row("Error", str(call_result.error.get("message", "Empty error")))
                table.add_row("Error trace", str(call_result.error.get("trace", "Empty trace")))
            result = ToolGlobals.client.functions.calls.get_response(
                call_id=call_result.id or 0, function_id=function.id
            )
            table.add_row("Result", str(json.dumps(result, indent=2, sort_keys=True)))
            logs = ToolGlobals.client.functions.calls.get_logs(call_id=call_result.id or 0, function_id=function.id)
            table.add_row("Logs", str(logs))
            print(table)
        return True

    def run_local_function(
        self,
        ToolGlobals: CDFToolConfig,
        source_path: Path,
        external_id: str,
        payload: str,
        build_env_name: str,
        schedule: str | None = None,
        rebuild_env: bool = False,
        verbose: bool = False,
        no_cleanup: bool = False,
    ) -> bool:
        try:
            import venv
        except ImportError:
            if _RUNNING_IN_BROWSER:
                print("  [bold red]ERROR:[/] This functionality is not supported in a browser environment.")
                return False
            raise

        system_config = SystemYAML.load_from_directory(source_path, build_env_name)
        config = BuildConfigYAML.load_from_directory(source_path, build_env_name)
        print(f"[bold]Building for environment {build_env_name} using {source_path!s} as sources...[/bold]")
        config.set_environment_variables()
        build_dir = Path(tempfile.mkdtemp(prefix="build.", suffix=".tmp", dir=Path.cwd()))

        found = False
        for function_dir in source_path.glob("**/functions"):
            if not function_dir.is_dir():
                continue
            for path in function_dir.iterdir():
                if path.is_dir() and path.name == external_id:
                    config.environment.selected = [module_from_path(path)]
                    found = True
                    break

        if not found:
            print(f"  [bold red]ERROR:[/] Could not find function with external id {external_id}, exiting.")
            return False
        BuildCommand().build_config(
            build_dir=build_dir,
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=True,
            verbose=False,
        )
        virtual_env_dir = Path(source_path / f".venv.{external_id}")
        if not virtual_env_dir.exists() or rebuild_env:
            print(f"  Creating virtual environment in {virtual_env_dir}...")
            venv.create(env_dir=virtual_env_dir.as_posix(), with_pip=True, system_site_packages=False)
            req_file = build_dir / "functions" / external_id / "requirements.txt"
            if req_file.exists():
                if platform.system() == "Windows":
                    process = subprocess.run(
                        [
                            str(virtual_env_dir / "Scripts" / "pip"),
                            "install",
                            "-r",
                            str(req_file),
                        ],
                        capture_output=True,
                        shell=True,
                    )
                else:
                    process = subprocess.run(
                        [
                            f"{virtual_env_dir}/bin/pip",
                            "--python",
                            f"{virtual_env_dir}/bin/python",
                            "install",
                            "--disable-pip-version-check",
                            "-r",
                            f"{req_file}",
                        ],
                        capture_output=True,
                    )
                if process.returncode != 0:
                    print(
                        "  [bold red]ERROR:[/] Failed to install requirements in virtual environment: ",
                        process.stderr.decode("utf-8"),
                    )
                    if not no_cleanup:
                        shutil.rmtree(build_dir)
                    return False
                if verbose:
                    print(process.stdout.decode("utf-8"))

        if verbose:
            print(f"  [bold]Loading function from {build_dir}...[/]")
        function_loader: FunctionLoader = FunctionLoader.create_loader(ToolGlobals, build_dir)
        function = None
        for filepath in function_loader.find_files():
            functions = function_loader.load_resource(
                Path(filepath), ToolGlobals=ToolGlobals, skip_validation=True
            ) or FunctionWriteList([])
            if not isinstance(functions, FunctionWriteList):
                functions = FunctionWriteList([functions])
            for func in functions:
                if func.external_id == external_id:
                    function = func
        if not function:
            print(
                f"  [bold red]ERROR:[/] Could not find function with external id {external_id} in the build directory, exiting."
            )
            if not no_cleanup:
                shutil.rmtree(build_dir)
            return False
        handler_file = Path(build_dir) / "functions" / external_id / func.function_path  # type: ignore[operator]
        if not handler_file.exists():
            print(f"  [bold red]ERROR:[/] Could not find handler file {handler_file}, exiting.")
            if not no_cleanup:
                shutil.rmtree(build_dir)
            return False
        if verbose:
            print("  [bold]Creating environment and tmpmain.py to run...[/]")
        # Create environment to transfer to sub-process
        environ = {
            "CDF_CLUSTER": ToolGlobals.environ("CDF_CLUSTER"),
            "CDF_PROJECT": ToolGlobals.environ("CDF_PROJECT"),
        }
        if ToolGlobals.environ("CDF_TOKEN", fail=False):
            environ["CDF_TOKEN"] = ToolGlobals.environ("CDF_TOKEN")
        else:
            schedule_loader = FunctionScheduleLoader.create_loader(ToolGlobals, build_dir)
            for filepath in schedule_loader.find_files():
                schedule_loader.load_resource(
                    Path(filepath), ToolGlobals=ToolGlobals, skip_validation=False
                ) or FunctionScheduleWriteList([])
            if schedule_loader.extra_configs.get(f"{external_id}:{schedule}", {}).get("authentication"):
                environ.update(
                    {
                        "IDP_CLIENT_ID": schedule_loader.extra_configs[f"{external_id}:{schedule}"][
                            "authentication"
                        ].get("clientId", ""),
                        "IDP_CLIENT_SECRET": schedule_loader.extra_configs[f"{external_id}:{schedule}"][
                            "authentication"
                        ].get("clientSecret", ""),
                    },
                )
            else:
                environ.update(
                    {
                        "IDP_CLIENT_ID": ToolGlobals.environ("IDP_CLIENT_ID"),
                        "IDP_CLIENT_SECRET": ToolGlobals.environ("IDP_CLIENT_SECRET"),
                    }
                )
            if ToolGlobals.environ("IDP_TOKEN_URL", fail=False):
                environ["IDP_TOKEN_URL"] = ToolGlobals.environ("IDP_TOKEN_URL")
            if ToolGlobals.environ("CDF_URL", fail=False):
                environ["CDF_URL"] = ToolGlobals.environ("CDF_URL")
            if ToolGlobals.environ("IDP_TENANT_ID", fail=False):
                environ["IDP_TENANT_ID"] = ToolGlobals.environ("IDP_TENANT_ID")
            if ToolGlobals.environ("IDP_AUDIENCE", fail=False):
                environ["IDP_AUDIENCE"] = ToolGlobals.environ("IDP_AUDIENCE")
            if ToolGlobals.environ("IDP_SCOPES", fail=False):
                environ["IDP_SCOPES"] = ToolGlobals.environ("IDP_SCOPES")
        if function.env_vars is not None and len(function.env_vars) > 0:
            for var, value in function.env_vars.items():
                environ[var] = value
        # Create temporary main file to execute
        (handler_file.parent / "tmpmain.py").write_text(
            """
from pathlib import Path
from handler import handle
import json
import inspect
import os
from collections import OrderedDict

from cognite.client import CogniteClient


def get_args(fn, handle_args):
    params = inspect.signature(fn).parameters
    kwargs = OrderedDict()
    for p in params.values():
        if p.name in handle_args:
            kwargs[p.name] = handle_args[p.name]
    return kwargs

if __name__ == "__main__":
    client = CogniteClient.default_oauth_client_credentials(
        client_id=os.getenv('IDP_CLIENT_ID'),
        client_secret=os.getenv('IDP_CLIENT_SECRET'),
        project=os.getenv('CDF_PROJECT'),
        cdf_cluster=os.getenv('CDF_CLUSTER'),
        tenant_id=os.getenv('IDP_TENANT_ID'),
        client_name="cognite-toolkit",
    )
    data = json.loads(Path("./in.json").read_text())
    args = get_args(handle, {
        "client": client,
        "data": data,
        "secrets": {},
        "function_call_info": {"local": True}
    })
    out = handle(**args)
    Path("./out.json").write_text(json.dumps(out))

    """
        )
        try:
            (Path(build_dir) / "functions" / external_id / "in.json").write_text(json.dumps(json.loads(payload)))
        except Exception:
            print(f"  [bold red]ERROR:[/] Could not decode your payload as json: {payload}")
            print('Remember to escape, example: --payload={\\"name\\": \\"test\\"}')
            if not no_cleanup:
                shutil.rmtree(build_dir)
            return False
        print("[bold]Running function locally...[/]")
        print("-------------------------------")
        if platform.system() == "Windows":
            python_exe = Path(virtual_env_dir / "Scripts" / "python.exe").absolute()
        else:
            python_exe = Path(virtual_env_dir / "bin" / "python").absolute()
        if verbose:
            print(f"  [bold]Running function with {python_exe}...[/]")

        if platform.system() == "Windows" and (system_root := os.environ.get("SYSTEMROOT")):
            # This is needed to run python on Windows.
            environ["SYSTEMROOT"] = system_root
        if platform.system() == "Windows":
            environ = {k: str(v) for k, v in environ.items()}

        process_run = subprocess.run(
            [
                str(python_exe),
                "-Xfrozen_modules=off",
                "tmpmain.py",
            ],
            capture_output=True,
            shell=True if platform.system() == "Windows" else False,
            env=environ,
            cwd=Path(build_dir) / "functions" / external_id,
        )

        out, err = process_run.stdout.decode("utf-8"), process_run.stderr.decode("utf-8")

        if process_run.returncode != 0:
            if "ModuleNotFoundError: No module named 'cognite'" in err:
                print(
                    "  [bold red]ERROR:[/] Could not find the Cognite SDK available in your function, check requirements.txt and try , try --rebuild-env:",
                    err,
                )
            else:
                print(
                    "  [bold red]ERROR:[/] Failed to run function: ",
                    err,
                    out,
                )
            if not no_cleanup:
                shutil.rmtree(build_dir)
            return False
        if (outfile := Path(build_dir) / "functions" / external_id / "out.json").exists():
            out_data = json.loads(outfile.read_text())
            print("  [bold]Function output:[/]")
            print(json.dumps(out_data, indent=2, sort_keys=True))
        else:
            print(
                "  [bold red]ERROR:[/] Could not get output from function.",
            )
        print("-------------------------------")
        print(f"[bold]Function {external_id} run completed with the following log: [/bold]")
        print("------------")
        print(out)
        if not no_cleanup:
            shutil.rmtree(build_dir)
        return True


class RunTransformationCommand(ToolkitCommand):
    def run_transformation(self, ToolGlobals: CDFToolConfig, external_ids: str | list[str]) -> bool:
        """Run a transformation in CDF"""
        if isinstance(external_ids, str):
            external_ids = [external_ids]
        session = get_oneshot_session(ToolGlobals.client)
        if session is None:
            print("[bold red]ERROR:[/] Could not get a oneshot session.")
            return False
        try:
            transformations: TransformationList = ToolGlobals.client.transformations.retrieve_multiple(
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
            ToolGlobals.client.transformations.update(transformations)
        except Exception as e:
            print("[bold red]ERROR:[/] Could not update transformations with oneshot session.")
            print(e)
            return False
        for transformation in transformations:
            try:
                job = ToolGlobals.client.transformations.run(
                    transformation_external_id=transformation.external_id, wait=False
                )
                print(f"Running transformation {transformation.external_id}, status {job.status}...")
            except Exception as e:
                print(f"[bold red]ERROR:[/] Could not run transformation {transformation.external_id}.")
                print(e)
        return True
