from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.commands import BuildV2Command, DeployV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class TestBuildDeployFunction:
    FUNCTION_CODE = """from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    print("Function 'fn_data_modeling_only_function' called with data:", data)
    return {
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}

"""
    REQUIREMENT_TXT = "cognite-sdk>=7.85.0"

    def test_build_deploy_function(
        self, toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse, tmp_path: Path
    ) -> None:
        config = tmp_path / MODULES / "fun_module" / "functions" / "first.function.yaml"
        config.parent.mkdir(parents=True, exist_ok=True)
        external_id = "smoke_test_deploy_function"
        config.write_text(f"""name: Smoke test deploy function
externalId: {external_id}
owner: doctrino
description: This function is deployed
dataSetExternalId: {smoke_dataset.external_id}
metadata:
  version: v1
runtime: py312""")
        code_path = config.parent / external_id / "handler.py"
        code_path.parent.mkdir(parents=True, exist_ok=True)
        code_path.write_text(self.FUNCTION_CODE)
        requirements_txt = config.parent / external_id / "requirements.txt"
        requirements_txt.write_text(self.REQUIREMENT_TXT)

        BuildV2Command(skip_tracking=True).build(
            parameters=BuildParameters(
                organization_dir=tmp_path,
                build_dir=tmp_path / "build",
                user_selected_modules=["fun_module"],
            ),
            client=toolkit_client,
        )
        env_vars = EnvironmentVariables.create_from_environment()
        DeployV2Command(skip_tracking=True).deploy(env_vars=env_vars, user_build_dir=tmp_path / "build")

        toolkit_client.tool.functions.delete([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
