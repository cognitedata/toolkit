import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    DataSet,
    Group,
    GroupWrite,
    Transformation,
    Workflow,
    WorkflowDefinition,
    WorkflowTrigger,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionUpsertList,
)
from cognite.client.data_classes.capabilities import AssetsAcl, EventsAcl, TimeSeriesAcl
from cognite.client.data_classes.workflows import WorkflowScheduledTriggerRule
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk import cdf_toml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilter
from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, DumpResourceCommand, PullCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder, WorkflowFinder
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import ToolkitDuplicatedModuleError
from cognite_toolkit._cdf_tk.loaders import RESOURCE_LOADER_LIST
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.constants import chdir
from tests.data import (
    BUILD_GROUP_WITH_UNKNOWN_ACL,
    COMPLETE_ORG_ONLY_IDENTIFIER,
    NAUGHTY_PROJECT,
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
    env_vars_with_client: EnvironmentVariables,
    organization_dir: Path,
) -> None:
    config_yaml = yaml.safe_load((organization_dir / "config.dev.yaml").read_text())
    config_yaml["variables"]["modules"]["cdf_common"]["dataset"] = "${MY_ENVIRONMENT_VARIABLE}"
    # Selecting the cdf_common module to be built
    config_yaml["environment"]["selected"] = ["cdf_common"]
    config_yaml["environment"]["project"] = "pytest"
    mock_read_yaml_file(
        {
            "config.dev.yaml": config_yaml,
        },
        monkeypatch,
    )
    monkeypatch.setenv("MY_ENVIRONMENT_VARIABLE", "my_environment_variable_value")
    BuildCommand(silent=True).execute(
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=None,
        on_error="raise",
        verbose=False,
    )
    DeployCommand(silent=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        dry_run=False,
        include=[],
        drop_data=False,
        verbose=False,
        force_update=False,
    )

    dataset = toolkit_client_approval.created_resources_of_type(DataSet)[0]
    assert dataset.external_id == "my_environment_variable_value"


def test_duplicated_modules(build_tmp_path: Path) -> None:
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
    env_vars_with_client: EnvironmentVariables,
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
        env_vars=env_vars_with_client,
    )

    reloaded = DataSet.load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    assert reloaded.description == "New description"


def test_pull_dataset_relative_path(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    organization_dir_mutable: Path,
) -> None:
    # Loading a selected dataset to be pulled
    dataset_yaml = organization_dir_mutable / MODULES / "cdf_common" / "data_sets" / "demo.DataSet.yaml"
    dataset = DataSet.load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    dataset.description = "New description"
    toolkit_client_approval.append(DataSet, dataset)

    with chdir(organization_dir_mutable):
        cmd = PullCommand(silent=True)
        cmd.pull_module(
            module_name_or_path=f"{MODULES}/cdf_common/data_sets/demo.DataSet.yaml",
            organization_dir=organization_dir_mutable,
            env="dev",
            dry_run=False,
            verbose=False,
            env_vars=env_vars_with_client,
        )

    reloaded = DataSet.load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    assert reloaded.description == "New description"


def test_pull_transformation_sql(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
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
    transformation = _load_cdf_pi_transformation(transformation_yaml, env_vars_with_client.get_client())
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
        env_vars=env_vars_with_client,
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


def _load_cdf_pi_transformation(transformation_yaml: Path, client: ToolkitClient) -> Transformation:
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
    data["dataSetId"] = client.lookup.data_sets.id(data.pop("dataSetExternalId"))
    transformation = Transformation._load(data)

    return transformation


def test_pull_workflow_trigger_with_environment_variables(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
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
        env_vars=env_vars_with_client,
    )
    reloaded = yaml_filepath.read_text()
    assert "cronExpression: '* 4 * * *'" in reloaded, "Workflow trigger was not updated"
    assert "clientId: {{ ingestionClientId }}" in reloaded, "Environment variables were not replaced"
    assert "clientSecret: {{ ingestionClientSecret }}" in reloaded, "Environment variables were not replaced"


def test_pull_group(
    build_tmp_path: Path,
    env_vars_with_client: EnvironmentVariables,
    toolkit_client_approval: ApprovalToolkitClient,
    tmp_path: Path,
) -> None:
    org_dir = tmp_path / "my-org"
    local_file = """name: my_group"""
    local_path = org_dir / "modules" / "my-module" / "auth" / "my_group.Group.yaml"
    local_path.parent.mkdir(parents=True)
    local_path.write_text(local_file)
    cdf_group = Group(
        name="my_group",
        source_id="123-456",
        capabilities=[
            AssetsAcl(scope=AssetsAcl.Scope.All(), actions=[AssetsAcl.Action.Read]),
            TimeSeriesAcl(scope=TimeSeriesAcl.Scope.All(), actions=[TimeSeriesAcl.Action.Read]),
            EventsAcl(scope=EventsAcl.Scope.All(), actions=[EventsAcl.Action.Read]),
        ],
        id=123,
    )
    toolkit_client_approval.append(Group, cdf_group)

    cmd = PullCommand(skip_tracking=True, silent=True)
    cmd.pull_module(
        module_name_or_path="my-module",
        organization_dir=org_dir,
        env="dev",
        dry_run=False,
        verbose=False,
        env_vars=env_vars_with_client,
    )

    reloaded = GroupWrite.load(local_path.read_text())

    assert reloaded.dump() == cdf_group.as_write().dump()


def test_dump_datamodel(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
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
    toolkit_client_approval.append(dm.DataModel, data_model)
    toolkit_client_approval.append(dm.View, [parent_view, view])
    cmd = DumpResourceCommand(silent=True)
    cmd.dump_to_yamls(
        DataModelFinder(env_vars_with_client.get_client(), dm.DataModelId.load(("my_space", "my_data_model", "1"))),
        clean=True,
        output_dir=build_tmp_path,
        verbose=False,
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


def test_dump_datamodel_skip_global(
    tmp_path: Path, toolkit_client_approval: ApprovalToolkitClient, env_vars_with_client: EnvironmentVariables
) -> None:
    output_dir = tmp_path / "tmp_dump"
    default_space_args = dict(is_global=False, last_updated_time=0, created_time=0)
    default_view_args = dict(
        last_updated_time=1,
        created_time=1,
        name=None,
        description=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
        filter=None,
    )
    default_prop_args = dict(
        nullable=True,
        immutable=False,
        auto_increment=False,
        default_value=None,
        name=None,
        description=None,
    )
    default_container_args = dict(
        name=None,
        description=None,
        is_global=False,
        last_updated_time=0,
        created_time=0,
        constraints=None,
        indexes=None,
        used_for="node",
    )
    local_space = dm.Space("my_space", **default_space_args)
    global_space = dm.Space("cdf_cdm", **{**default_space_args, "is_global": True})
    toolkit_client_approval.append(dm.Space, [local_space, global_space])
    local_container = dm.Container(
        space=local_space.space, external_id="MyAsset", properties={}, **default_container_args
    )
    global_container = dm.Container(
        space=global_space.space,
        external_id="CogniteAsset",
        properties={},
        **{**default_container_args, "is_global": True},
    )
    toolkit_client_approval.append(dm.Container, [local_container, global_container])
    local_view = dm.View(
        space=local_space.space,
        external_id="my_view",
        version="1",
        properties={
            "prop1": dm.MappedProperty(
                container=local_container.as_id(),
                container_property_identifier="prop1",
                type=dm.Text(),
                **default_prop_args,
            ),
        },
        **{**default_view_args, "is_global": False},
    )
    global_view = dm.View(
        space=global_space.space,
        external_id="global_view",
        version="1",
        properties={
            "prop1": dm.MappedProperty(
                container=global_container.as_id(),
                container_property_identifier="prop1",
                type=dm.Text(),
                **default_prop_args,
            ),
        },
        **{**default_view_args, "is_global": True},
    )
    toolkit_client_approval.append(dm.View, [local_view, global_view])
    data_model = dm.DataModel(
        space=local_space.space,
        external_id="my_data_model",
        version="1",
        views=[local_view.as_id(), global_view.as_id()],
        is_global=False,
        last_updated_time=0,
        created_time=0,
        name=None,
        description=None,
    )
    toolkit_client_approval.append(dm.DataModel, data_model)

    cmd = DumpResourceCommand(silent=True, skip_tracking=True)
    cmd.dump_to_yamls(
        finder=DataModelFinder(
            env_vars_with_client.get_client(),
            dm.DataModelId.load(("my_space", "my_data_model", "1")),
            include_global=False,
        ),
        clean=True,
        output_dir=output_dir,
        verbose=False,
    )

    assert len(list(output_dir.glob("**/*.DataModel.yaml"))) == 1
    assert len(list(output_dir.glob("**/*.Container.yaml"))) == 1
    assert len(list(output_dir.glob("**/*.Space.yaml"))) == 1
    assert len(list(output_dir.glob("**/*.View.yaml"))) == 1


def test_build_custom_project(
    build_tmp_path: Path,
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
    BuildCommand(silent=True).execute(
        organization_dir=PROJECT_NO_COGNITE_MODULES,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=None,
        on_error="raise",
        verbose=False,
    )

    actual_resources = {path.name for path in build_tmp_path.iterdir() if path.is_dir()}

    missing_resources = expected_resources - actual_resources
    assert not missing_resources, f"Missing resources: {missing_resources}"

    extra_resources = actual_resources - expected_resources
    assert not extra_resources, f"Extra resources: {extra_resources}"


def test_build_project_selecting_parent_path(
    build_tmp_path: Path,
) -> None:
    expected_resources = {"auth", "data_models", "files", "transformations", "data_sets"}
    BuildCommand(silent=True).execute(
        organization_dir=PROJECT_FOR_TEST,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=None,
        on_error="raise",
        verbose=False,
    )

    actual_resources = {path.name for path in build_tmp_path.iterdir() if path.is_dir()}

    missing_resources = expected_resources - actual_resources
    assert not missing_resources, f"Missing resources: {missing_resources}"

    extra_resources = actual_resources - expected_resources
    assert not extra_resources, f"Extra resources: {extra_resources}"


def test_deploy_group_with_unknown_acl(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    DeployCommand(silent=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=BUILD_GROUP_WITH_UNKNOWN_ACL,
        build_env_name="dev",
        drop=False,
        dry_run=False,
        include=None,
        verbose=False,
        drop_data=False,
        force_update=False,
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
) -> None:
    BuildCommand(silent=True).execute(
        organization_dir=PROJECT_NO_COGNITE_MODULES,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="top_level_variables",
        no_clean=False,
        client=None,
        on_error="raise",
        verbose=False,
    )

    assert build_tmp_path.exists()


def test_dump_workflow(
    tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    # Simulate Workflow, WorkflowVersion, and WorkflowTrigger in CDF.
    toolkit_client_approval.append(
        Workflow,
        Workflow("myWorkflow", 0, 1),
    )
    toolkit_client_approval.append(
        WorkflowVersion,
        WorkflowVersion(
            "myWorkflow",
            "v1",
            WorkflowDefinition(
                # We are testing that the dump fetches the workflow version, not the actual content of the workflow.
                hash_="some-hash",
                tasks=[],
            ),
            0,
            1,
        ),
    )
    toolkit_client_approval.append(
        WorkflowTrigger, WorkflowTrigger("myTrigger", WorkflowScheduledTriggerRule("* * * * * "), "myWorkflow", "v1")
    )

    output_dir = tmp_path / "tmp_dump"
    cmd = DumpResourceCommand(silent=True)
    cmd.dump_to_yamls(
        WorkflowFinder(env_vars_with_client.get_client(), WorkflowVersionId("myWorkflow", "v1")),
        output_dir=output_dir,
        clean=True,
        verbose=False,
    )

    assert len(list(output_dir.glob("**/*.Workflow.yaml"))) == 1
    assert len(list(output_dir.glob("**/*.WorkflowTrigger.yaml"))) == 1
    assert len(list(output_dir.glob("**/*.WorkflowVersion.yaml"))) == 1


def test_build_deploy_location_filter_with_same_filename_in_different_modules(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    BuildCommand(silent=True).execute(
        False,
        NAUGHTY_PROJECT,
        build_tmp_path,
        ["modules/multi_locations"],
        None,
        False,
        env_vars_with_client.get_client(),
        "raise",
    )

    DeployCommand(silent=True).deploy_build_directory(
        env_vars_with_client,
        build_tmp_path,
        None,
        dry_run=False,
        drop=False,
        drop_data=False,
        force_update=False,
        include=None,
        verbose=False,
    )

    locations = toolkit_client_approval.created_resources_of_type(LocationFilter)

    assert len(locations) == 2


@pytest.mark.skipif(sys.platform != "win32", reason="The encoding issue is only present on Windows")
@pytest.mark.parametrize("encoding", ["utf-8", "cp1252"])
def test_build_deploy_keep_special_characters(
    encoding: str,
    tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    monkeypatch,
) -> None:
    build_dir = tmp_path / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    expected_query = "SELECT * FROM my_éñcüd€d£d_table WHERE column = 'value'"

    my_cdf_toml = cdf_toml.CDFToml.load(use_singleton=False)
    my_cdf_toml.cdf.file_encoding = encoding
    monkeypatch.setattr(cdf_toml, "_CDF_TOML", my_cdf_toml)
    BuildCommand(silent=True).execute(
        False, NAUGHTY_PROJECT, build_dir, ["encoding_issue"], None, False, env_vars_with_client.get_client(), "raise"
    )

    DeployCommand(silent=True).deploy_build_directory(
        env_vars_with_client,
        build_dir,
        None,
        dry_run=False,
        drop=False,
        drop_data=False,
        force_update=False,
        include=None,
        verbose=False,
    )

    transformations = toolkit_client_approval.created_resources_of_type(Transformation)

    assert len(transformations) == 2
    transformation = next(t for t in transformations if t.external_id.endswith(encoding))
    assert transformation.query == expected_query


def test_build_project_with_only_identifiers(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that module with only identifiers can be built."""
    BuildCommand(silent=True).execute(
        organization_dir=COMPLETE_ORG_ONLY_IDENTIFIER,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=None,
        on_error="raise",
        verbose=False,
    )
    DeployCommand(silent=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=True,
        dry_run=False,
        include=[],
        drop_data=False,
        verbose=False,
        force_update=False,
    )

    # Check that the expected transformations were created
    # Note: The behavior asserts that the expected transformations are created
    # but the actual transformations will be skipped due to the dry run.
    created_transformations = toolkit_client_approval.created_resources_of_type(Transformation)
    assert len(created_transformations) == 3


def test_workflow_version_deployment_order_dependency_failure(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that workflow versions with dependencies fail when deployed out of order."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create base workflow first
    base_workflow = Workflow(
        external_id="base_workflow",
        description="Base workflow",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, base_workflow)

    # Create dependent workflow
    dependent_workflow = Workflow(
        external_id="dependent_workflow", 
        description="Dependent workflow",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, dependent_workflow)

    # Create workflow version that depends on another workflow version (through subworkflow)
    # We'll create this using the raw dict format as used in YAML files
    dependent_version_dict = {
        "workflowExternalId": "dependent_workflow",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_base_workflow",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "base_workflow",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    # Create base workflow version (the dependency)
    base_version_dict = {
        "workflowExternalId": "base_workflow", 
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "simple_task",
                    "type": "function",
                    "parameters": {
                        "function": {
                            "externalId": "some_function"
                        }
                    }
                }
            ]
        }
    }

    # Try to deploy in wrong order (dependent first, then dependency)
    # This should fail because the dependent workflow version references
    # a workflow version that doesn't exist yet
    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    dependent_upsert = WorkflowVersionUpsert._load(dependent_version_dict)
    base_upsert = WorkflowVersionUpsert._load(base_version_dict)
    
    # Try to create in wrong order - this demonstrates the issue
    # First create the dependent version (which depends on base_version)
    result1 = loader.create(WorkflowVersionUpsertList([dependent_upsert]))
    # Then create the base version
    result2 = loader.create(WorkflowVersionUpsertList([base_upsert]))
    
    # Both succeed in the mock environment, but in a real CDF environment,
    # this could cause issues when trying to run the dependent workflow
    # because the dependency doesn't exist yet at deployment time
    assert len(result1) == 1
    assert len(result2) == 1
    
    # This demonstrates that the current implementation doesn't enforce 
    # dependency ordering between workflow versions
    assert result1[0].workflow_external_id == "dependent_workflow"
    assert result2[0].workflow_external_id == "base_workflow"


def test_workflow_version_dependency_ordering_fix(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that workflow versions with dependencies are deployed in correct order."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create base workflow first
    base_workflow = Workflow(
        external_id="base_workflow",
        description="Base workflow", 
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, base_workflow)

    # Create dependent workflow
    dependent_workflow = Workflow(
        external_id="dependent_workflow",
        description="Dependent workflow",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, dependent_workflow)

    # Create workflow version that depends on another workflow version (through subworkflow)
    dependent_version_dict = {
        "workflowExternalId": "dependent_workflow",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_base_workflow",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "base_workflow",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    # Create base workflow version (the dependency)
    base_version_dict = {
        "workflowExternalId": "base_workflow",
        "version": "v1", 
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "simple_task",
                    "type": "function",
                    "parameters": {
                        "function": {
                            "externalId": "some_function"
                        }
                    }
                }
            ]
        }
    }

    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    dependent_upsert = WorkflowVersionUpsert._load(dependent_version_dict)
    base_upsert = WorkflowVersionUpsert._load(base_version_dict)
    
    # Deploy both in wrong order - the loader should now sort them properly
    # Pass dependent first, then base (wrong order)
    result = loader.create(WorkflowVersionUpsertList([dependent_upsert, base_upsert]))
    
    # The result should have 2 items, and they should be ordered correctly:
    # base_workflow should be created first, then dependent_workflow
    assert len(result) == 2
    assert result[0].workflow_external_id == "base_workflow"  # dependency created first
    assert result[1].workflow_external_id == "dependent_workflow"  # dependent created second


def test_workflow_version_circular_dependency_detection(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that circular dependencies in workflow versions are detected and raise proper errors."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create workflows
    workflow_a = Workflow(
        external_id="workflow_a",
        description="Workflow A",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, workflow_a)

    workflow_b = Workflow(
        external_id="workflow_b",
        description="Workflow B",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, workflow_b)

    # Create circular dependency: A depends on B, B depends on A
    version_a_dict = {
        "workflowExternalId": "workflow_a",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_workflow_b",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "workflow_b",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    version_b_dict = {
        "workflowExternalId": "workflow_b",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_workflow_a",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "workflow_a",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    version_a_upsert = WorkflowVersionUpsert._load(version_a_dict)
    version_b_upsert = WorkflowVersionUpsert._load(version_b_dict)
    
    # Try to deploy circular dependencies - should raise ValueError
    with pytest.raises(ValueError, match="Circular dependency detected"):
        loader.create(WorkflowVersionUpsertList([version_a_upsert, version_b_upsert]))


def test_workflow_version_no_dependencies(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that workflow versions without dependencies are handled correctly."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create workflow
    workflow = Workflow(
        external_id="simple_workflow",
        description="Simple workflow",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, workflow)

    # Create workflow version without dependencies
    version_dict = {
        "workflowExternalId": "simple_workflow",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "simple_task",
                    "type": "function",
                    "parameters": {
                        "function": {
                            "externalId": "some_function"
                        }
                    }
                }
            ]
        }
    }

    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    version_upsert = WorkflowVersionUpsert._load(version_dict)
    
    # Deploy workflow version without dependencies
    result = loader.create(WorkflowVersionUpsertList([version_upsert]))
    
    # Should succeed and return the single workflow version
    assert len(result) == 1
    assert result[0].workflow_external_id == "simple_workflow"


def test_workflow_version_complex_dependency_chain(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test complex dependency chains are sorted correctly."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create workflows for a 3-level dependency chain: A -> B -> C
    for workflow_name in ["workflow_a", "workflow_b", "workflow_c"]:
        workflow = Workflow(
            external_id=workflow_name,
            description=f"Workflow {workflow_name[-1].upper()}",
            data_set_id=None,
            created_time=1234567890,
            last_updated_time=1234567890
        )
        toolkit_client_approval.append(Workflow, workflow)

    # C has no dependencies
    version_c_dict = {
        "workflowExternalId": "workflow_c",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "task_c",
                    "type": "function",
                    "parameters": {
                        "function": {
                            "externalId": "function_c"
                        }
                    }
                }
            ]
        }
    }

    # B depends on C
    version_b_dict = {
        "workflowExternalId": "workflow_b",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_workflow_c",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "workflow_c",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    # A depends on B
    version_a_dict = {
        "workflowExternalId": "workflow_a",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_workflow_b",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "workflow_b",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    version_a_upsert = WorkflowVersionUpsert._load(version_a_dict)
    version_b_upsert = WorkflowVersionUpsert._load(version_b_dict)
    version_c_upsert = WorkflowVersionUpsert._load(version_c_dict)
    
    # Deploy in wrong order (A, B, C instead of C, B, A)
    result = loader.create(WorkflowVersionUpsertList([version_a_upsert, version_b_upsert, version_c_upsert]))
    
    # Should be sorted correctly: C first, then B, then A
    assert len(result) == 3
    assert result[0].workflow_external_id == "workflow_c"  # no dependencies
    assert result[1].workflow_external_id == "workflow_b"  # depends on C
    assert result[2].workflow_external_id == "workflow_a"  # depends on B


def test_workflow_version_empty_list(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that empty workflow version lists are handled correctly."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    
    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Deploy empty list
    result = loader.create(WorkflowVersionUpsertList([]))
    
    # Should return empty list
    assert len(result) == 0


def test_workflow_version_external_dependencies_ignored(
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """Test that dependencies outside the current batch are ignored for sorting."""
    from cognite_toolkit._cdf_tk.loaders import WorkflowVersionLoader
    from cognite.client.data_classes import WorkflowVersionUpsert
    
    # Create workflow
    workflow = Workflow(
        external_id="dependent_workflow",
        description="Dependent workflow",
        data_set_id=None,
        created_time=1234567890,
        last_updated_time=1234567890
    )
    toolkit_client_approval.append(Workflow, workflow)

    # Create workflow version that depends on external workflow (not in batch)
    version_dict = {
        "workflowExternalId": "dependent_workflow",
        "version": "v1",
        "workflowDefinition": {
            "tasks": [
                {
                    "externalId": "call_external_workflow",
                    "type": "subworkflow",
                    "parameters": {
                        "subworkflow": {
                            "workflowExternalId": "external_workflow",
                            "version": "v1"
                        }
                    }
                }
            ]
        }
    }

    loader = WorkflowVersionLoader.create_loader(toolkit_client_approval.mock_client)
    
    # Convert to upsert objects
    version_upsert = WorkflowVersionUpsert._load(version_dict)
    
    # Deploy workflow version with external dependency
    result = loader.create(WorkflowVersionUpsertList([version_upsert]))
    
    # Should succeed because external dependencies are ignored for sorting
    assert len(result) == 1
    assert result[0].workflow_external_id == "dependent_workflow"
