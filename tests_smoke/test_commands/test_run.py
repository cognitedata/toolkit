from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import RunFunctionCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class TestRunFunctionLocal:
    FUNCTION_CODE = """from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    print("Function called with data:", data)
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}

"""
    REQUIREMENT_TXT = "cognite-sdk>=7.85.0"

    def test_run_local_function(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        functions_dir = tmp_path / MODULES / "fun_module" / "functions"
        functions_dir.mkdir(parents=True)
        external_id = "smoke_test_run_local_function"
        schedule_name = "smoke-daily"

        (functions_dir / "first.function.yaml").write_text(
            f"""name: Smoke test run local function
externalId: {external_id}
owner: toolkit-smoke
description: This function is executed locally
metadata:
  version: v1
runtime: py312
functionPath: ./handler.py
secrets:
  mysecret: smoke-secret
"""
        )
        (functions_dir / "first.schedule.yaml").write_text(
            f"""name: {schedule_name}
functionExternalId: {external_id}
description: Schedule used as the local run data source
cronExpression: "0 8 * * *"
data:
  breakfast: egg and bacon
  lunch: a chicken
"""
        )
        code_dir = functions_dir / external_id
        code_dir.mkdir()
        (code_dir / "handler.py").write_text(self.FUNCTION_CODE)
        (code_dir / "requirements.txt").write_text(self.REQUIREMENT_TXT)

        env_vars = EnvironmentVariables.create_from_environment()
        env_vars._client = toolkit_client

        try:
            RunFunctionCommand(skip_tracking=True, silent=True).run_local(
                env_vars=env_vars,
                organization_dir=tmp_path,
                build_env_name=None,
                external_id=external_id,
                data_source=schedule_name,
                rebuild_env=True,
            )
        except ToolkitError as e:
            raise AssertionError(
                f"Running function {external_id!r} locally failed. "
                "Toolkit could not set up the virtual environment, install requirements, "
                f"or execute the handler. Details: {e}"
            ) from e
