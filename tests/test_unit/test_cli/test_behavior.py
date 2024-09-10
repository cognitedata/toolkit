from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
import typer
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import GroupWrite, Transformation, TransformationWrite
from pytest import MonkeyPatch
from typer import Context

from cognite_toolkit._cdf import build, deploy, dump_datamodel_cmd, pull_transformation_cmd
from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import ToolkitDuplicatedModuleError
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
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
    config_yaml["variables"]["cicd_clientId"] = "${MY_ENVIRONMENT_VARIABLE}"
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

    build(
        typer_context,
        organization_dir=organization_dir,
        build_dir=str(build_tmp_path),
        build_env_name="dev",
        no_clean=False,
    )
    deploy(
        typer_context,
        build_dir=str(build_tmp_path),
        build_env_name="dev",
        interactive=False,
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


def test_pull_transformation(
    build_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    toolkit_client_approval: ApprovalToolkitClient,
    cdf_tool_mock: CDFToolConfig,
    typer_context: typer.Context,
    organization_dir_mutable: Path,
) -> None:
    # Loading a selected transformation to be pulled
    transformation_yaml = (
        organization_dir_mutable
        / "modules"
        / "examples"
        / "cdf_example_pump_asset_hierarchy"
        / "transformations"
        / "pump_asset_hierarchy-load-collections_pump.yaml"
    )
    loader = TransformationLoader.create_loader(cdf_tool_mock, None)

    def load_transformation() -> TransformationWrite:
        # Injecting variables into the transformation file, so we can load it.
        original = transformation_yaml.read_text()
        content = original.replace("{{data_set}}", "ds_test")
        content = content.replace("{{cicd_clientId}}", "123")
        content = content.replace("{{cicd_clientSecret}}", "123")
        content = content.replace("{{cicd_tokenUri}}", "123")
        content = content.replace("{{cdfProjectName}}", "123")
        content = content.replace("{{cicd_scopes}}", "scope")
        content = content.replace("{{cicd_audience}}", "123")
        transformation_yaml.write_text(content)

        transformation = loader.load_resource(transformation_yaml, cdf_tool_mock, skip_validation=True)
        # Write back original content
        transformation_yaml.write_text(original)
        return cast(TransformationWrite, transformation)

    loaded = load_transformation()

    # Simulate a change in the transformation in CDF.
    loaded.name = "New transformation name"
    read_transformation = Transformation.load(loaded.dump())
    toolkit_client_approval.append(Transformation, read_transformation)

    pull_transformation_cmd(
        typer_context,
        organization_dir=organization_dir_mutable,
        external_id=read_transformation.external_id,
        env="dev",
        dry_run=False,
    )

    after_loaded = load_transformation()

    assert after_loaded.name == "New transformation name"


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
            "prop2": dm.MappedProperty(
                container=container.as_id(),
                container_property_identifier="prop2",
                type=dm.Float64(),
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
        views=[view, parent_view],
        created_time=0,
        last_updated_time=0,
        description=None,
        name=None,
        is_global=False,
    )
    toolkit_client_approval.append(dm.Space, space)
    toolkit_client_approval.append(dm.Container, container)
    toolkit_client_approval.append(dm.View, view)
    toolkit_client_approval.append(dm.DataModel, data_model)

    dump_datamodel_cmd(
        typer_context,
        space="my_space",
        external_id="my_data_model",
        version="1",
        clean=True,
        output_dir=str(build_tmp_path),
    )

    assert len(list(build_tmp_path.glob("**/*.datamodel.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.container.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.space.yaml"))) == 1
    view_files = list(build_tmp_path.glob("**/*.view.yaml"))
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
    build(
        typer_context,
        organization_dir=str(PROJECT_NO_COGNITE_MODULES),
        build_dir=str(build_tmp_path),
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
    build(
        typer_context,
        organization_dir=str(PROJECT_FOR_TEST),
        build_dir=str(build_tmp_path),
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
    deploy(
        typer_context,
        build_dir=str(BUILD_GROUP_WITH_UNKNOWN_ACL),
        build_env_name="dev",
        interactive=False,
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
    build(
        typer_context,
        organization_dir=str(PROJECT_NO_COGNITE_MODULES),
        build_dir=str(build_tmp_path),
        build_env_name="top_level_variables",
        no_clean=False,
    )

    assert build_tmp_path.exists()
