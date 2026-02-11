import os
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from cognite.client.data_classes import (
    DataSet,
    Group,
    GroupWrite,
    Transformation,
)
from cognite.client.data_classes.capabilities import AssetsAcl, EventsAcl, TimeSeriesAcl
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk import cdf_toml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerReference,
    ContainerResponse,
    DataModelReference,
    DataModelResponse,
    Float64Property,
    SpaceReference,
    SpaceResponse,
    TextProperty,
    ViewCorePropertyResponse,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import ConstraintOrIndexState
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    ScheduleTriggerRule,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    SubworkflowTaskParameters,
    Task,
    TransformationRef,
    TransformationTaskParameters,
    WorkflowDefinition,
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)
from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, DumpResourceCommand, PullCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder, WorkflowFinder
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_LIST, LocationFilterCRUD, WorkflowVersionCRUD
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import ToolkitDuplicatedModuleError
from cognite_toolkit._cdf_tk.tk_warnings import MissingDependencyWarning
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from tests.constants import CDF_PROJECT, chdir
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
    buildable_modules: Path,
) -> None:
    config_yaml = yaml.safe_load((buildable_modules / "config.dev.yaml").read_text())
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
        organization_dir=buildable_modules,
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

    dataset = toolkit_client_approval.created_resources_of_type(DataSetResponse)[0]
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
    buildable_modules_mutable: Path,
) -> None:
    # Loading a selected dataset to be pulled
    dataset_yaml = buildable_modules_mutable / MODULES / "cdf_common" / "data_sets" / "demo.DataSet.yaml"
    raw = yaml.safe_load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    raw.update({"id": 42, "createdTime": 0, "lastUpdatedTime": 0})
    dataset = DataSetResponse._load(raw)
    dataset.description = "New description"
    toolkit_client_approval.append(DataSetResponse, dataset)

    cmd = PullCommand(silent=True)
    cmd.pull_module(
        module_name_or_path=dataset_yaml,
        organization_dir=buildable_modules_mutable,
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
    buildable_modules_mutable: Path,
) -> None:
    # Loading a selected dataset to be pulled
    dataset_yaml = buildable_modules_mutable / MODULES / "cdf_common" / "data_sets" / "demo.DataSet.yaml"
    raw = yaml.safe_load(dataset_yaml.read_text().replace("{{ dataset }}", "ingestion"))
    raw.update({"id": 42, "createdTime": 0, "lastUpdatedTime": 0})
    dataset = DataSetResponse._load(raw)
    dataset.description = "New description"
    toolkit_client_approval.append(DataSetResponse, dataset)

    with chdir(buildable_modules_mutable):
        cmd = PullCommand(silent=True)
        cmd.pull_module(
            module_name_or_path=f"{MODULES}/cdf_common/data_sets/demo.DataSet.yaml",
            organization_dir=buildable_modules_mutable,
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
    buildable_modules_mutable: Path,
) -> None:
    # Loading a selected transformation to be pulled
    transformation_yaml = (
        buildable_modules_mutable
        / "modules"
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
        organization_dir=buildable_modules_mutable,
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
    buildable_modules_mutable: Path,
) -> None:
    # Loading a selected workflow trigger to be pulled
    yaml_filepath = (
        buildable_modules_mutable / "modules" / "cdf_ingestion" / "workflows" / "trigger.WorkflowTrigger.yaml"
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
    response_dict = yaml.safe_load(vars_replaced)
    response_dict["triggerRule"]["cronExpression"] = "* 4 * * *"
    response_dict.pop("authentication", None)
    response_dict["createdTime"] = 0
    response_dict["lastUpdatedTime"] = 1
    response_dict["isPaused"] = False
    trigger = WorkflowTriggerResponse._load(response_dict)
    toolkit_client_approval.append(WorkflowTriggerResponse, trigger)

    cmd = PullCommand(silent=True)
    cmd.pull_module(
        module_name_or_path=yaml_filepath,
        organization_dir=buildable_modules_mutable,
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
    default_config_dev_yaml: str,
    env_vars_with_client: EnvironmentVariables,
    toolkit_client_approval: ApprovalToolkitClient,
    tmp_path: Path,
) -> None:
    org_dir = tmp_path / "my-org"

    local_file = """name: my_group"""
    local_path = org_dir / "modules" / "my-module" / "auth" / "my_group.Group.yaml"
    local_path.parent.mkdir(parents=True)
    local_path.write_text(local_file)
    (org_dir / "config.dev.yaml").write_text(default_config_dev_yaml, encoding="utf-8")

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
    container_ref = ContainerReference(space="my_space", external_id="my_container")
    space = SpaceResponse(space="my_space", is_global=False, last_updated_time=0, created_time=0)
    container = ContainerResponse(
        space="my_space",
        external_id="my_container",
        properties={
            "prop1": ContainerPropertyDefinition(type=TextProperty()),
            "prop2": ContainerPropertyDefinition(type=Float64Property()),
        },
        is_global=False,
        last_updated_time=0,
        created_time=0,
        used_for="node",
    )
    parent_view = ViewResponse(
        space="my_space",
        external_id="parent_view",
        version="1",
        properties={
            "prop2": ViewCorePropertyResponse(
                container=container_ref,
                container_property_identifier="prop2",
                type=Float64Property(),
                nullable=True,
                auto_increment=False,
                immutable=False,
                constraint_state=ConstraintOrIndexState(),
            )
        },
        last_updated_time=0,
        created_time=0,
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[container_ref],
    )

    view = ViewResponse(
        space="my_space",
        external_id="my_view",
        version="1",
        properties={
            "prop1": ViewCorePropertyResponse(
                container=container_ref,
                container_property_identifier="prop1",
                type=TextProperty(),
                nullable=True,
                auto_increment=False,
                immutable=False,
                constraint_state=ConstraintOrIndexState(),
            ),
        },
        last_updated_time=0,
        created_time=0,
        implements=[parent_view.as_id()],
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[container_ref],
    )
    data_model = DataModelResponse(
        space="my_space",
        external_id="my_data_model",
        version="1",
        views=[view.as_id(), parent_view.as_id()],
        created_time=0,
        last_updated_time=0,
        is_global=False,
    )
    toolkit_client_approval.append(SpaceResponse, space)
    toolkit_client_approval.append(ContainerResponse, container)
    toolkit_client_approval.append(DataModelResponse, data_model)
    toolkit_client_approval.append(ViewResponse, [parent_view, view])
    cmd = DumpResourceCommand(silent=True)
    cmd.dump_to_yamls(
        DataModelFinder(
            env_vars_with_client.get_client(),
            DataModelReference(space="my_space", external_id="my_data_model", version="1"),
        ),
        clean=True,
        output_dir=build_tmp_path,
        verbose=False,
    )

    assert len(list(build_tmp_path.glob("**/*.DataModel.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.Container.yaml"))) == 1
    assert len(list(build_tmp_path.glob("**/*.Space.yaml"))) == 1
    view_files = list(build_tmp_path.glob("**/*.View.yaml"))
    assert len(view_files) == 2
    loaded_views = [ViewRequest.model_validate(yaml.safe_load(f.read_text())) for f in view_files]
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
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
    )
    default_prop_args = dict(
        nullable=True,
        immutable=False,
        auto_increment=False,
        constraint_state=ConstraintOrIndexState(),
    )
    default_container_args = dict(
        is_global=False,
        last_updated_time=0,
        created_time=0,
        used_for="node",
    )
    local_space = SpaceResponse(space="my_space", **default_space_args)
    global_space = SpaceResponse(space="cdf_cdm", **{**default_space_args, "is_global": True})
    toolkit_client_approval.append(SpaceResponse, [local_space, global_space])
    local_container_ref = ContainerReference(space=local_space.space, external_id="MyAsset")
    local_container = ContainerResponse(
        space=local_space.space, external_id="MyAsset", properties={}, **default_container_args
    )
    global_container_ref = ContainerReference(space=global_space.space, external_id="CogniteAsset")
    global_container = ContainerResponse(
        space=global_space.space,
        external_id="CogniteAsset",
        properties={},
        **{**default_container_args, "is_global": True},
    )
    toolkit_client_approval.append(ContainerResponse, [local_container, global_container])
    local_view = ViewResponse(
        space=local_space.space,
        external_id="my_view",
        version="1",
        properties={
            "prop1": ViewCorePropertyResponse(
                container=local_container_ref,
                container_property_identifier="prop1",
                type=TextProperty(),
                **default_prop_args,
            ),
        },
        mapped_containers=[local_container_ref],
        **{**default_view_args, "is_global": False},
    )
    global_view = ViewResponse(
        space=global_space.space,
        external_id="global_view",
        version="1",
        properties={
            "prop1": ViewCorePropertyResponse(
                container=global_container_ref,
                container_property_identifier="prop1",
                type=TextProperty(),
                **default_prop_args,
            ),
        },
        mapped_containers=[global_container_ref],
        **{**default_view_args, "is_global": True},
    )
    toolkit_client_approval.append(ViewResponse, [local_view, global_view])
    data_model = DataModelResponse(
        space=local_space.space,
        external_id="my_data_model",
        version="1",
        views=[local_view.as_id(), global_view.as_id()],
        is_global=False,
        last_updated_time=0,
        created_time=0,
    )
    toolkit_client_approval.append(DataModelResponse, data_model)

    cmd = DumpResourceCommand(silent=True, skip_tracking=True)
    cmd.dump_to_yamls(
        finder=DataModelFinder(
            env_vars_with_client.get_client(),
            DataModelReference(space="my_space", external_id="my_data_model", version="1"),
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
        "data_modeling",
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
    expected_resources = {"auth", "data_modeling", "files", "transformations", "data_sets"}
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
        WorkflowResponse,
        WorkflowResponse(external_id="myWorkflow", created_time=0, last_updated_time=1),
    )
    toolkit_client_approval.append(
        WorkflowVersionResponse,
        WorkflowVersionResponse(
            workflow_external_id="myWorkflow",
            version="v1",
            workflow_definition=WorkflowDefinition(
                # We are testing that the dump fetches the workflow version, not the actual content of the workflow.
                tasks=[],
            ),
            created_time=0,
            last_updated_time=1,
        ),
    )
    toolkit_client_approval.append(
        WorkflowTriggerResponse,
        WorkflowTriggerResponse(
            external_id="myTrigger",
            trigger_rule=ScheduleTriggerRule(cron_expression="* * * * *"),
            workflow_external_id="myWorkflow",
            workflow_version="v1",
            created_time=0,
            last_updated_time=1,
            is_paused=False,
        ),
    )

    output_dir = tmp_path / "tmp_dump"
    cmd = DumpResourceCommand(silent=True)
    cmd.dump_to_yamls(
        WorkflowFinder(
            env_vars_with_client.get_client(), WorkflowVersionId(workflow_external_id="myWorkflow", version="v1")
        ),
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

    locations = toolkit_client_approval.created_resources_of_type(LocationFilterResponse)

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


def test_location_filter_deployment_order(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    child = """externalId: child
name: Child Location Filter
parentExternalId: parent
dataModels:
  - externalId: CogniteProcessIndustries
    space: cdf_idm
    version: v1
instanceSpaces:
  - instance-space-secondary
dataModelingType: DATA_MODELING_ONLY
"""
    parent = """externalId: parent
name: Parent Location Filter
dataModels:
  - externalId: CogniteCore
    space: cdf_cdm
    version: v1
instanceSpaces:
  - instance-space-main
dataModelingType: DATA_MODELING_ONLY
"""
    org = build_tmp_path.parent / "org"
    resource_folder_child = org / MODULES / "my_first" / LocationFilterCRUD.folder_name
    resource_folder_parent = org / MODULES / "my_second" / LocationFilterCRUD.folder_name
    # Default behavior of Toolkit is to respect the order of the files, however, this tests ensures
    # that Toolkit does a topological sort of the location filter before deploying them.
    child_file = resource_folder_child / f"1.child.{LocationFilterCRUD.kind}.yaml"
    parent_file = resource_folder_parent / f"2.parent.{LocationFilterCRUD.kind}.yaml"
    child_file.parent.mkdir(parents=True, exist_ok=True)
    child_file.write_text(child, encoding="utf-8")
    parent_file.parent.mkdir(parents=True, exist_ok=True)
    parent_file.write_text(parent, encoding="utf-8")

    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=org,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
    )

    DeployCommand(silent=True, skip_tracking=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=False,
        dry_run=False,
        include=[],
        drop_data=False,
        verbose=False,
        force_update=False,
    )

    # Verify that the workflow was created in the correct order, parent before child.
    filters = toolkit_client_approval.created_resources_of_type(LocationFilterResponse)
    assert len(filters) == 2
    assert [loc_filter.external_id for loc_filter in filters] == ["parent", "child"]


def test_build_project_with_only_identifiers(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    """In the cdf modules pull command, we have to be able to build a project that only has identifiers
    without raising any errors.
    """
    built_modules = BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=COMPLETE_ORG_ONLY_IDENTIFIER,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name="dev",
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
    )

    # Loading the local resources as it is done in the PullCommand
    for loader_cls in RESOURCE_CRUD_LIST:
        loader = loader_cls.create_loader(env_vars_with_client.get_client())
        built_resources = built_modules.get_resources(
            None,
            loader.folder_name,
            loader.kind,
        )
        _ = PullCommand._get_local_resource_dict_by_id(built_resources, loader, {})


def test_workflow_deployment_order(
    build_tmp_path: Path,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
) -> None:
    subworkflow = WorkflowVersionRequest(
        workflow_external_id="mySubWorkflow",
        version="v1",
        workflow_definition=WorkflowDefinition(
            tasks=[
                Task(
                    external_id="task1",
                    type="transformation",
                    parameters=TransformationTaskParameters(
                        transformation=TransformationRef(external_id="someTransformation"),
                    ),
                )
            ],
        ),
    )
    main_workflow = WorkflowVersionRequest(
        workflow_external_id="myWorkflow",
        version="v1",
        workflow_definition=WorkflowDefinition(
            tasks=[
                Task(
                    external_id="subworkflowTask",
                    type="subworkflow",
                    parameters=SubworkflowTaskParameters(
                        subworkflow=WorkflowVersionId(
                            workflow_external_id=subworkflow.workflow_external_id,
                            version=subworkflow.version,
                        ),
                    ),
                ),
            ],
        ),
    )
    org = build_tmp_path.parent / "org"
    resource_folder = org / MODULES / "my_workflow_module" / WorkflowVersionCRUD.folder_name
    # Default behavior of Toolkit is to respect the order of the files, however, this tests ensures
    # that Toolkit does a topological sort of the workflows before deploying them.
    main_workflow_file = resource_folder / f"1.{main_workflow.workflow_external_id}.{WorkflowVersionCRUD.kind}.yaml"
    subworkflow_file = resource_folder / f"2.{subworkflow.workflow_external_id}.{WorkflowVersionCRUD.kind}.yaml"
    main_workflow_file.parent.mkdir(parents=True, exist_ok=True)
    main_workflow_file.write_text(yaml_safe_dump(main_workflow.dump()), encoding="utf-8")
    subworkflow_file.write_text(yaml_safe_dump(subworkflow.dump()), encoding="utf-8")

    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=org,
        build_dir=build_tmp_path,
        selected=None,
        build_env_name=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        on_error="raise",
    )

    DeployCommand(silent=True, skip_tracking=True).deploy_build_directory(
        env_vars=env_vars_with_client,
        build_dir=build_tmp_path,
        build_env_name="dev",
        drop=False,
        dry_run=False,
        include=[],
        drop_data=False,
        verbose=False,
        force_update=False,
    )

    # Verify that the workflow was created in the correct order, subworkflow before main.
    # Note: Resources are stored under the response type name but contain request objects
    workflows = toolkit_client_approval.created_resources_of_type(WorkflowVersionResponse)
    assert len(workflows) == 2
    assert [workflow.workflow_external_id for workflow in workflows] == [
        subworkflow.workflow_external_id,
        main_workflow.workflow_external_id,
    ]


def test_warning_missing_dependency(
    default_config_dev_yaml: str,
    toolkit_client_approval: ApprovalToolkitClient,
    env_vars_with_client: EnvironmentVariables,
    tmp_path: Path,
) -> None:
    group_yaml = """name: scoped_group
sourceId: '1234567890123456789'
metadata:
  origin: cognite-toolkit
capabilities:
- dataModelsAcl:
    actions:
    - READ
    scope:
      spaceIdScope:
        spaceIds:
        - my_non_existent_space
"""

    my_org = tmp_path / "my_org"
    yaml_filepath = my_org / "modules" / "my_module" / "auth" / "scoped_group.Group.yaml"
    yaml_filepath.parent.mkdir(parents=True, exist_ok=True)
    yaml_filepath.write_text(group_yaml, encoding="utf-8")

    (my_org / "config.dev.yaml").write_text(default_config_dev_yaml, encoding="utf-8")

    cmd = BuildCommand(silent=True, skip_tracking=True)
    with patch.dict(os.environ, {"CDF_PROJECT": CDF_PROJECT}):
        cmd.execute(
            verbose=False,
            organization_dir=my_org,
            build_dir=tmp_path / "build",
            selected=None,
            build_env_name="dev",
            no_clean=False,
            client=env_vars_with_client.get_client(),
            on_error="raise",
        )
    assert len(cmd.warning_list) == 1
    warning = cmd.warning_list[0]
    assert isinstance(warning, MissingDependencyWarning)
    assert warning.identifier == SpaceReference(space="my_non_existent_space")
    assert warning.required_by == {("scoped_group", yaml_filepath.relative_to(my_org))}
