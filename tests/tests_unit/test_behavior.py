from pathlib import Path

import typer
import yaml
from cognite.client.data_classes import Transformation
from pytest import MonkeyPatch

from cognite_toolkit import _version
from cognite_toolkit.cdf import build, deploy
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.tests_unit.approval_client import ApprovalCogniteClient
from tests.tests_unit.utils import mock_read_yaml_file


def test_inject_custom_environmental_variables(
    local_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    cognite_client_approval: ApprovalCogniteClient,
    cdf_tool_config: CDFToolConfig,
    typer_context: typer.Context,
    init_project: Path,
) -> None:
    config_yaml = yaml.safe_load((init_project / "config.yaml").read_text())
    config_yaml["cognite_modules"]["cicd_clientId"] = "${MY_ENVIRONMENT_VARIABLE}"
    mock_read_yaml_file(
        {
            "environments.yaml": {
                "dev": {
                    "project": "pytest",
                    "type": "dev",
                    # Selecting a module with a transformation that uses the cicd_clientId variable
                    "deploy": ["cdf_infield_location"],
                },
                "__system": {"cdf_toolkit_version": _version.__version__},
            },
            "config.yaml": config_yaml,
        },
        monkeypatch,
    )
    monkeypatch.setenv("MY_ENVIRONMENT_VARIABLE", "my_environment_variable_value")

    build(
        typer_context,
        source_dir=str(init_project),
        build_dir=str(local_tmp_path),
        build_env="dev",
        clean=True,
    )
    deploy(
        typer_context,
        build_dir=str(local_tmp_path),
        build_env="dev",
        interactive=False,
        drop=True,
        dry_run=False,
        include=[],
    )

    transformation = cognite_client_approval.created_resources_of_type(Transformation)[0]
    assert transformation.source_oidc_credentials.client_id == "my_environment_variable_value"
