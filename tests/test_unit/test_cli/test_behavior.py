from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
import typer
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import DataSet, GroupWrite, Transformation, WorkflowTrigger
from pytest import MonkeyPatch
from typer import Context

from cognite_toolkit._cdf_tk.apps import CoreApp, DumpApp
from cognite_toolkit._cdf_tk.commands import BuildCommand, PullCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import ToolkitDuplicatedModuleError
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import (
    BUILD_GROUP_WITH_UNKNOWN_ACL,
    PROJECT_FOR_TEST,
    PROJECT_NO_COGNITE_MODULES,
    PROJECT_WITH_DUPLICATES,
)
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import mock_read_yaml_file


def test_inject_custom_environmental_variables(
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    typer_context: typer.Context,
    organization_dir: Path,
) -> None:
    config_yaml = yaml.safe_load((organization_dir / "config.dev.yaml").read_text())
    config_yaml["variables"]["modules"]["examples"]["cicd_clientId"] = "${MY_ENVIRONMENT_VARIABLE}"
    config_yaml["variables"]["modules"]["infield"]["cicd_clientId"] = "${MY_ENVIRONMENT_VARIABLE}"
    # Selecting a module with a transformation that uses the cicd_clientId variable
    config_yaml["environment"]["selected"] = ["cdf_infield_location"]
    config_yaml["environment"]["project"] = "pytest"
    mock_read_yaml_file(
        {
            "config.dev.yaml": config_yaml,
        },
        monkeypatch,
    )
    monkeypatch.setenv("MY_ENVIRONMENT_VARIABLE", "my_environment_variable_value")
    app = CoreApp()
    app.build(
        typer_context,
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
    )
    app.deploy(
        typer_context,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        dry_run=False,
        include=[],
    )

    transformation = toolkit_client_approval.created_resources_of_type(Transformation)[0]
    assert transformation.source_oidc_credentials.client_id == "my_environment_variable_value"


def test_duplicated_modules(build_tmp_path: Path, typer_context: typer.Context) -> None:
    config = MagicMock(spec=BuildConfigYAML)
    config.environment = MagicMock(spec=Environment)
    config.environment.name = "dev"
    config.environment.selected = ["module1"]
    with pytest.raises(ToolkitDuplicatedModuleError) as err:
        BuildCommand().build_config(
            build_dir=build_tmp_path,
            organization_dir=PROJECT_WITH_DUPLICATES,
            config=config,
            packages={},
        )
    l1, l2, l3, l4, l5 = map(str.strip, str(err.value).splitlines())
    assert l1 == "Ambiguous module selected in config.dev.yaml:"
    assert l2 == "module1 exists in:"
    assert l3 == "modules/examples/module1"
    assert l4 == "modules/models/module1"
    assert l5.startswith("You can use the path syntax to disambiguate between modules with the same name")


def test_pull_dataset(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    organization_dir_mutable: Path,
) -> None:
    # Loading a selected dataset to be pulled
    dataset_yaml = organization_dir_mutable / MODULES / "cdf_common" / "data_sets" / "demo.DataSet.yaml"
    dataset = DataSet.load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    dataset.description = "New description"
    toolkit_client_approval.append(DataSet, dataset)

    cmd = PullCommand(silent=True)
    cmd.pull_module(
        module_name_or_path=dataset_yaml,
        organization_dir=organization_dir_mutable,
        env="dev",
        dry_run=False,
        verbose=False,
        ToolGlobals=cdf_tool_mock,
    )

    reloaded = DataSet.load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    assert reloaded.description == "New description"


def test_pull_transformation_sql(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    organization_dir_mutable: Path,
) -> None:
    # Loading a selected transformation to be pulled
    transformation_yaml = (
        organization_dir_mutable
        / "modules"
        / "sourcesystem"
        / "cdf_pi"
        / "transformations"
        / "population"
        / "timeseries.Transformation.yaml"
    )
    source_yaml = transformation_yaml.read_text()
    transformation = _load_cdf_pi_transformation(transformation_yaml, cdf_tool_mock)
    new_query = """select
  someValue as externalId,
  name as name,
  'string' as type,

from `ingestion`.`timeseries_metadata`"""
    transformation.query = new_query

    toolkit_client_approval.append(Transformation, transformation)
    cmd = PullCommand(silent=True)
    cmd.pull_module(
        module_name_or_path=transformation_yaml,
        organization_dir=organization_dir_mutable,
        env="dev",
        dry_run=False,
        verbose=False,
        ToolGlobals=cdf_tool_mock,
    )
    sql_file = transformation_yaml.with_suffix(".sql")
    assert sql_file.exists()
    assert sql_file.read_text() == new_query.replace("ingestion", "{{ rawSourceDatabase }}"), "SQL file was not updated"

    target_yaml = transformation_yaml.read_text()
    # Cleanup file endings.
    while target_yaml.endswith("\n"):
        target_yaml = target_yaml[:-1]
    while source_yaml.endswith("\n"):
        source_yaml = source_yaml[:-1]
    assert target_yaml == source_yaml, "Transformation file should not be updated"


def _load_cdf_pi_transformation(transformation_yaml: Path, cdf_tool_mock: CDFToolConfig) -> Transformation:
    variables = [
        ("dataset", "ingestion"),
        ("schemaSpace", "sp_enterprise_process_industry"),
        ("instanceSpace", "springfield_instances"),
        ("organization", "YourOrg"),
        ("timeseriesTransformationExternalId", "pi_timeseries_springfield_aveva_pi"),
        ("sourceName", "Springfield AVEVA PI"),
    ]
    raw_transformation = transformation_yaml.read_text()
    for key, value in variables:
        raw_transformation = raw_transformation.replace(f"{{{{ {key} }}}}", value)
    data = yaml.safe_load(raw_transformation)
    data["dataSetId"] = cdf_tool_mock.toolkit_client.lookup.data_sets.id(data.pop("dataSetExternalId"))
    transformation = Transformation._load(data)

    return transformation


def test_pull_workflow_trigger_with_environment_variables(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    organization_dir_mutable: Path,
) -> None:
    # Loading a selected workflow trigger to be pulled
    yaml_filepath = (
        organization_dir_mutable / "modules" / "cdf_ingestion" / "workflows" / "trigger.WorkflowTrigger.yaml"
    )
    source_yaml = yaml_filepath.read_text()
    vars_replaced = source_yaml
    for key, value in [
        ("{{ workflow }}", "ingestion"),
        # These two secrets are replaced by environment variables
        # that are then replaced with the actual values.
        ("{{ ingestionClientId }}", "this-is-the-ingestion-client-id"),
        ("{{ ingestionClientSecret }}", "this-is-the-ingestion-client-secret"),
    ]:
        vars_replaced = vars_replaced.replace(key, value)
    trigger_dict = yaml.safe_load(vars_replaced)
    trigger_dict["triggerRule"]["cronExpression"] = "* 4 * * *"
    trigger = WorkflowTrigger._load(trigger_dict)
    toolkit_client_approval.append(WorkflowTrigger, trigger)

    cmd = PullCommand(silent=True)
    cmd.pull_module(
        module_name_or_path=yaml_filepath,
        organization_dir=organization_dir_mutable,
        env="dev",
        dry_run=False,
        verbose=False,
        ToolGlobals=cdf_tool_mock,
    )
    reloaded = yaml_filepath.read_text()
    assert "cronExpression: '* 4 * * *'" in reloaded, "Workflow trigger was not updated"
    assert "clientId: {{ ingestionClientId }}" in reloaded, "Environment variables were not replaced"
    assert "clientSecret: {{ ingestionClientSecret }}" in reloaded, "Environment variables were not replaced"


def test_dump_datamodel(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    typer_context: typer.Context,
) -> None:
    # Create a datamodel and append it to the approval client
    space = dm.Space("my_space", is_global=False, last_updated_time=0, created_time=0)
    container = dm.Container(
        space="my_space",
        external_id="my_container",
        name=None,
        description=None,
        properties={"prop1": dm.ContainerProperty(type=dm.Text()), "prop2": dm.ContainerProperty(type=dm.Float64())},
        is_global=False,
        last_updated_time=0,
        created_time=0,
        used_for="node",
        constraints=None,
        indexes=None,
    )
    parent_view = dm.View(
        space="my_space",
        external_id="parent_view",
        version="1",
        properties={
            "prop2": dm.MappedProperty(
                container=container.as_id(),
                container_property_identifier="prop2",
                type=dm.Float64(),
                nullable=True,
                auto_increment=False,
                immutable=False,
            )
        },
        last_updated_time=0,
        created_time=0,
        description=None,
        name=None,
        filter=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
    )

    view = dm.View(
        space="my_space",
        external_id="my_view",
        version="1",
        properties={
            "prop1": dm.MappedProperty(
                container=container.as_id(),
                container_property_identifier="prop1",
                type=dm.Text(),
                nullable=True,
                auto_increment=False,
                immutable=False,
            ),
        },
        last_updated_time=0,
        created_time=0,
        description=None,
        name=None,
        filter=None,
        implements=[parent_view.as_id()],
        writable=True,
        used_for="node",
        is_global=False,
    )
    data_model = dm.DataModel(
        space="my_space",
        external_id="my_data_model",
        version="1",
        views=[view.as_id(), parent_view.as_id()],
        created_time=0,
        last_updated_time=0,
        description=None,
        name=None,
        is_global=False,
    )
    toolkit_client_approval.append(dm.Space, space)
    toolkit_client_approval.append(dm.Container, container)
    toolkit_client_approval.append(dm.View, parent_view)
    toolkit_client_approval.append(dm.DataModel, data_model)
    toolkit_client_approval.append(dm.View, [parent_view, view])
    app = DumpApp()
    app.dump_datamodel_cmd(
        typer_context,
        data_model_id=["my_space", "my_data_model", "1"],
        clean=True,
        output_dir=build_tmp_path,
    )

    assert len(list(build_tmp_path.glob("**/*.DataModel.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.Container.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.Space.yaml"))) == 1
    view_files = list(build_tmp_path.glob("**/*.View.yaml"))
    assert len(view_files) == 2
    loaded_views = [dm.ViewApply.load(f.read_text()) for f in view_files]
    child_loaded = next(v for v in loaded_views if v.external_id == "my_view")
    assert child_loaded.implements[0] == parent_view.as_id()
    # The parent property should have been removed from the child view.
    assert len(child_loaded.properties) == 1


def test_build_custom_project(
    build_tmp_path: Path,
    typer_context: typer.Context,
) -> None:
    expected_resources = {
        "timeseries",
        "data_models",
        "data_sets",
        "raw",
        "extraction_pipelines",
        "transformations",
        "robotics",
    }
    app = CoreApp()
    app.build(
        typer_context,
        organization_dir=PROJECT_NO_COGNITE_MODULES,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
    )

    actual_resources = {path.name for path in build_tmp_path.iterdir() if path.is_dir()}

    missing_resources = expected_resources - actual_resources
    assert not missing_resources, f"Missing resources: {missing_resources}"

    extra_resources = actual_resources - expected_resources
    assert not extra_resources, f"Extra resources: {extra_resources}"


def test_build_project_selecting_parent_path(
    build_tmp_path: Path,
    typer_context: Context,
) -> None:
    expected_resources = {"auth", "data_models", "files", "transformations", "data_sets"}
    app = CoreApp()
    app.build(
        typer_context,
        organization_dir=PROJECT_FOR_TEST,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
    )

    actual_resources = {path.name for path in build_tmp_path.iterdir() if path.is_dir()}

    missing_resources = expected_resources - actual_resources
    assert not missing_resources, f"Missing resources: {missing_resources}"

    extra_resources = actual_resources - expected_resources
    assert not extra_resources, f"Extra resources: {extra_resources}"


def test_deploy_group_with_unknown_acl(
    typer_context: Context,
    toolkit_client_approval: ApprovalToolkitClient,
) -> None:
    app = CoreApp()
    app.deploy(
        typer_context,
        build_dir=BUILD_GROUP_WITH_UNKNOWN_ACL,
        build_env_name="dev",
        drop=False,
        dry_run=False,
        include=None,
        verbose=False,
    )

    groups = toolkit_client_approval.created_resources["Group"]
    assert len(groups) == 1
    group = cast(GroupWrite, groups[0])
    assert group.name == "my_group_with_unknown_acl"
    assert len(group.capabilities) == 1
    assert group.capabilities[0].dump() == {
        "someUnknownAcl": {
            "actions": ["UTTERLY_UNKNOWN"],
            "scope": {"unknownScope": {"with": ["some", {"strange": "structure"}]}},
        }
    }


def test_build_project_with_only_top_level_variables(
    build_tmp_path: Path,
    typer_context: typer.Context,
) -> None:
    app = CoreApp()
    app.build(
        typer_context,
        organization_dir=PROJECT_NO_COGNITE_MODULES,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="top_level_variables",
        no_clean=False,
    )

    assert build_tmp_path.exists()
