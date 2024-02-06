from pathlib import Path

import typer
import yaml
from cognite.client.data_classes import Transformation
from pytest import MonkeyPatch

from cognite_toolkit.cdf import build, deploy, pull_transformation_cmd
from cognite_toolkit.cdf_tk.load import TransformationLoader
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
    config_yaml = yaml.safe_load((init_project / "config.dev.yaml").read_text())
    config_yaml["modules"]["cognite_modules"]["cicd_clientId"] = "${MY_ENVIRONMENT_VARIABLE}"
    # Selecting a module with a transformation that uses the cicd_clientId variable
    config_yaml["environment"]["selected_modules_and_packages"] = ["cdf_infield_location"]
    config_yaml["environment"]["project"] = "pytest"
    mock_read_yaml_file(
        {
            "config.dev.yaml": config_yaml,
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


def test_pull_transformation(
    local_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    cognite_client_approval: ApprovalCogniteClient,
    cdf_tool_config: CDFToolConfig,
    typer_context: typer.Context,
    init_project: Path,
) -> None:
    # Loading a selected transformation to be pulled
    transformation_yaml = (
        init_project
        / "cognite_modules"
        / "examples"
        / "example_pump_asset_hierarchy"
        / "transformations"
        / "pump_asset_hierarchy-load-collections_pump.yaml"
    )

    # Injecting variables into the transformation file, so we can load it.
    content = transformation_yaml.read_text()
    content = content.replace("{{data_set}}", "ds_test")
    content = content.replace("{{cicd_clientId}}", "123")
    content = content.replace("{{cicd_clientSecret}}", "123")
    content = content.replace("{{cicd_tokenUri}}", "123")
    content = content.replace("{{cdfProjectName}}", "123")
    content = content.replace("{{cicd_scopes}}", "scope")
    content = content.replace("{{cicd_audience}}", "123")
    transformation_yaml.write_text(content)

    loader = TransformationLoader.create_loader(cdf_tool_config)

    loaded = loader.load_resource(transformation_yaml, cdf_tool_config, skip_validation=True)
    # Simulate a change in the transformation in CDF.
    loaded.name = "New transformation name"
    read_transformation = Transformation.load(loaded.dump())
    cognite_client_approval.append(Transformation, loaded)

    pull_transformation_cmd(
        typer_context,
        source_dir=str(init_project),
        external_id=read_transformation.external_id,
        env="dev",
        dry_run=False,
    )

    loaded = loader.load_resource(transformation_yaml, cdf_tool_config, skip_validation=True)

    assert loaded.name == "New transformation name"
