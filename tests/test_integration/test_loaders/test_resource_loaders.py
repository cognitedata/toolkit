import os
from asyncio import sleep
from contextlib import suppress
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    AssetWrite,
    AssetWriteList,
    ClientCredentials,
    DataPointSubscriptionWrite,
    DataSet,
    Function,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionScheduleWriteList,
    FunctionTaskParameters,
    GroupWrite,
    LabelDefinitionWrite,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
    WorkflowVersionUpsert,
    WorkflowVersionUpsertList,
    filters,
)
from cognite.client.data_classes.capabilities import IDScopeLowerCase, TimeSeriesAcl
from cognite.client.data_classes.data_modeling import ViewApply, ViewApplyList
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscriptionProperty,
    DatapointSubscriptionWriteList,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.extendable_cognite_file import (
    ExtendableCogniteFileApply,
    ExtendableCogniteFileApplyList,
)
from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    DataPostProcessingWrite,
    DataPostProcessingWriteList,
    RobotCapability,
    RobotCapabilityWrite,
    RobotCapabilityWriteList,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.loaders import (
    AssetLoader,
    CogniteFileLoader,
    DataModelLoader,
    DatapointSubscriptionLoader,
    FunctionScheduleLoader,
    GroupLoader,
    LabelLoader,
    NodeLoader,
    ResourceWorker,
    RobotCapabilityLoader,
    RoboticsDataPostProcessingLoader,
    TransformationLoader,
    ViewLoader,
    WorkflowVersionLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning, catch_warnings
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestFunctionScheduleLoader:
    # The function schedule service is fairly unstable, so we need to rerun the tests if they fail.
    @pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError"])
    def test_update_function_schedule(
        self,
        toolkit_client: ToolkitClient,
        toolkit_client_config: ToolkitClientConfig,
        dummy_function: Function,
        dummy_schedule: FunctionSchedule,
    ) -> None:
        loader = FunctionScheduleLoader(toolkit_client, None, None)
        function_schedule = dummy_schedule.as_write()

        function_schedule.description = (
            "Updated description."
            if function_schedule.description != "Updated description."
            else "Original description."
        )
        identifier = loader.get_id(function_schedule)
        assert isinstance(toolkit_client_config.credentials, OAuthClientCredentials)
        loader.authentication_by_id[identifier] = ClientCredentials(
            toolkit_client_config.credentials.client_id, toolkit_client_config.credentials.client_secret
        )

        # Function schedules cannot be updated, they must be deleted and recreated.
        loader.delete([identifier])
        loader.create(FunctionScheduleWriteList([function_schedule]))

        retrieved = loader.retrieve([identifier])
        if not retrieved or retrieved[0].description != function_schedule.description:
            # The service can be a bit slow in returning the updated description,
            # so we wait a bit and try again. (Eventual consistency)
            sleep(2)
            retrieved = loader.retrieve([identifier])

        assert retrieved, "Function schedule not found after update."
        assert retrieved[0].description == function_schedule.description

    def test_creating_schedule_then_print_ids(
        self, toolkit_client: ToolkitClient, toolkit_client_config: ToolkitClientConfig, dummy_function: Function
    ) -> None:
        local = FunctionScheduleWrite(
            name="test_creating_schedule_then_print_ids",
            cron_expression="0 7 * * TUE",
            function_external_id=dummy_function.external_id,
            description="This schedule should be ignored as it does not have a function_external_id",
        )
        loader = FunctionScheduleLoader(toolkit_client, None, None)
        assert isinstance(toolkit_client_config.credentials, OAuthClientCredentials)
        loader.authentication_by_id[loader.get_id(local)] = ClientCredentials(
            toolkit_client_config.credentials.client_id, toolkit_client_config.credentials.client_secret
        )

        created: FunctionSchedulesList | None = None
        try:
            created = loader.create(FunctionScheduleWriteList([local]))
            loader.get_ids(created)
        finally:
            if created:
                toolkit_client.functions.schedules.delete(created[0].id)


@pytest.fixture(scope="session")
def three_timeseries(cognite_client: CogniteClient) -> TimeSeriesList:
    ts_list = TimeSeriesWriteList(
        [
            TimeSeriesWrite(
                external_id=f"toolkit_test_timeseries_{i}",
                name=f"Toolkit Test TimeSeries {i}",
                is_step=False,
                is_string=False,
            )
            for i in range(1, 4)
        ]
    )
    retrieved = cognite_client.time_series.retrieve_multiple(
        external_ids=ts_list.as_external_ids(), ignore_unknown_ids=True
    )
    existing = set(retrieved.as_external_ids())
    to_create = [ts for ts in ts_list if ts.external_id not in existing]
    if to_create:
        created = cognite_client.time_series.create(to_create)
        retrieved.extend(created)
    return retrieved


class TestDatapointSubscriptionLoader:
    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        loader = DatapointSubscriptionLoader(cognite_client, None)
        delete_count = loader.delete(["non_existing"])
        assert delete_count == 0

    def test_create_update_delete_subscription(self, cognite_client: CogniteClient) -> None:
        sub = DataPointSubscriptionWrite(
            external_id=f"tmp_test_create_update_delete_subscription_{RUN_UNIQUE_ID}",
            partition_count=1,
            name="Initial name",
            filter=filters.Prefix(DatapointSubscriptionProperty.external_id, "ts_value"),
        )
        update = DataPointSubscriptionWrite(
            external_id=f"tmp_test_create_update_delete_subscription_{RUN_UNIQUE_ID}",
            partition_count=1,
            name="Updated name",
            filter=filters.Prefix(DatapointSubscriptionProperty.external_id, "ts_value"),
        )

        loader = DatapointSubscriptionLoader(cognite_client, None)

        try:
            created = loader.create(DatapointSubscriptionWriteList([sub]))
            assert len(created) == 1

            updated = loader.update(DatapointSubscriptionWriteList([update]))
            assert len(updated) == 1
            assert updated[0].name == "Updated name"
        finally:
            loader.delete([sub.external_id])

    def test_no_redeploy_ids_defined(
        self, toolkit_client: ToolkitClient, toolkit_dataset: DataSet, three_timeseries: TimeSeriesList
    ) -> None:
        definition_yaml = f"""externalId: toolkit_test_no_redeploy_ids_defined
name: Test no redeploy IDs defined
partitionCount: 1
dataSetExternalId: {toolkit_dataset.external_id}
timeSeriesIds:
- {three_timeseries[0].external_id}
- {three_timeseries[1].external_id}
- {three_timeseries[2].external_id}
"""
        loader = DatapointSubscriptionLoader.create_loader(toolkit_client)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = definition_yaml

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(resource_dict[0])
        assert isinstance(resource, DataPointSubscriptionWrite)
        if not loader.retrieve([resource.external_id]):
            _ = loader.create(DatapointSubscriptionWriteList([resource]))

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}


class TestLabelLoader:
    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        loader = LabelLoader(cognite_client, None)
        delete_count = loader.delete(["non_existing"])
        assert delete_count == 0

    def test_create_delete_label(self, toolkit_client: ToolkitClient) -> None:
        label = LabelDefinitionWrite(
            external_id=f"tmp_test_create_update_delete_label_{RUN_UNIQUE_ID}",
            name="Initial name",
        )
        loader = LabelLoader(toolkit_client, None)

        try:
            created = loader.create(LabelDefinitionWriteList([label]))
            assert len(created) == 1
        finally:
            loader.delete([label.external_id])


class TestAssetLoader:
    def test_create_delete_asset(self, cognite_client: CogniteClient) -> None:
        asset = AssetWrite(
            external_id=f"tmp_test_create_delete_asset_{RUN_UNIQUE_ID}",
            name="My Asset",
            description="My description",
        )

        loader = AssetLoader(cognite_client, None)

        try:
            created = loader.create(AssetWriteList([asset]))
            assert len(created) == 1

            delete_count = loader.delete([asset.external_id])
            assert delete_count == 1
        finally:
            # Ensure that the asset is deleted even if the test fails.
            cognite_client.assets.delete(external_id=asset.external_id, ignore_unknown_ids=True)


@pytest.fixture
def existing_robot_capability(toolkit_client: ToolkitClient) -> RobotCapability:
    write = RobotCapabilityWrite(
        name="integration_test_robot_capability",
        description="Test robot capability",
        external_id="integration_test_robot_capability",
        method="ptz",
        input_schema={},
        data_handling_schema={},
    )

    try:
        return toolkit_client.robotics.capabilities.retrieve(write.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.capabilities.create(write)


class TestRobotCapability:
    def test_retrieve_existing_and_not_existing(
        self, toolkit_client: ToolkitClient, existing_robot_capability: RobotCapability
    ) -> None:
        loader = RobotCapabilityLoader(toolkit_client, None)

        capabilities = loader.retrieve([existing_robot_capability.external_id, "non_existing_robot"])

        assert len(capabilities) == 1

    def test_create_update_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        loader = RobotCapabilityLoader(toolkit_client, None)

        original = RobotCapabilityWrite.load("""name: Read dial gauge
externalId: read_dial_gauge
method: read_dial_gauge
description: Original Description
inputSchema:
    $schema: http://json-schema.org/draft-07/schema#
    id: robotics/schemas/0.1.0/capabilities/ptz
    title: PTZ camera capability input
dataHandlingSchema:
    $schema: http://json-schema.org/draft-07/schema#
    id: robotics/schemas/0.1.0/data_handling/read_dial_gauge
    title: Read dial gauge data handling
""")

        update = RobotCapabilityWrite.load("""name: Read dial gauge
externalId: read_dial_gauge
method: read_dial_gauge
description: Original Description
inputSchema:
    $schema: http://json-schema.org/draft-07/schema#
    id: robotics/schemas/0.2.0/capabilities/ptz
    title: Updated PTZ camera capability input
dataHandlingSchema:
    $schema: http://json-schema.org/draft-07/schema#
    id: robotics/schemas/0.2.0/data_handling/read_dial_gauge
    title: Updated read dial gauge data handling
""")
        try:
            created = loader.create(RobotCapabilityWriteList([original]))
            assert len(created) == 1

            updated = loader.update(RobotCapabilityWriteList([update]))
            assert len(updated) == 1
            assert updated[0].input_schema == update.input_schema

            retrieved = loader.retrieve([original.external_id])
            assert len(retrieved) == 1
            assert retrieved[0].input_schema == update.input_schema
        finally:
            loader.delete([original.external_id])


class TestRobotDataPostProcessing:
    def test_create_update_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        loader = RoboticsDataPostProcessingLoader(toolkit_client, None)

        original = DataPostProcessingWrite.load("""name: Read dial gauge
externalId: read_dial_gauge
method: read_dial_gauge
description: Original Description
inputSchema:
  $schema: http://json-schema.org/draft-07/schema#
  id: robotics/schemas/0.1.0/capabilities/ptz
  title: PTZ camera capability input
  type: object
  properties:
    method:
      type: string
    parameters:
      type: object
      properties:
        tilt:
          type: number
          minimum: -90
          maximum: 90
        pan:
          type: number
          minimum: -180
          maximum: 180
        zoom:
          type: number
          minimum: 0
          maximum: 100
      required:
      - tilt
      - pan
      - zoom
  required:
  - method
  - parameters
  additionalProperties: false
""")

        update = DataPostProcessingWrite.load("""method: read_dial_gauge
name: Read dial gauge
externalId: read_dial_gauge
description: Read dial gauge from an image using Cognite Vision gauge reader
inputSchema:
  $schema: http://json-schema.org/draft-07/schema#
  id: robotics/schemas/0.1.0/data_postprocessing/read_dial_gauge
  title: Read dial gauge input
  type: object
  properties:
    image:
      type: object
      properties:
        method:
          type: string
        parameters:
          type: object
          properties:
            unit:
              type: string
            deadAngle:
              type: number
            minLevel:
              type: number
            maxLevel:
              type: number
      required:
        - method
        - parameters
      additionalProperties: false
  additionalProperties: false""")

        # Ensure the original is deleted even if the test fails
        loader.delete([original.external_id])

        try:
            created = loader.create(DataPostProcessingWriteList([original]))
            assert len(created) == 1

            updated = loader.update(DataPostProcessingWriteList([update]))
            assert len(updated) == 1
            assert updated[0].input_schema == update.input_schema

            retrieved = loader.retrieve([original.external_id])
            assert len(retrieved) == 1
            assert retrieved[0].input_schema == update.input_schema
        finally:
            loader.delete([original.external_id])


@pytest.fixture(scope="module")
def schema_space(toolkit_client: ToolkitClient) -> dm.Space:
    return toolkit_client.data_modeling.spaces.apply(
        dm.SpaceApply(
            space=f"sp_test_resource_loaders_{RUN_UNIQUE_ID}",
        )
    )


@pytest.fixture(scope="module")
def instance_space(toolkit_client: ToolkitClient) -> dm.Space:
    return toolkit_client.data_modeling.spaces.apply(
        dm.SpaceApply(
            space=f"sp_instances_{RUN_UNIQUE_ID}",
        )
    )


@pytest.fixture(scope="module")
def a_container(toolkit_client: ToolkitClient, schema_space: dm.Space) -> dm.Container:
    return toolkit_client.data_modeling.containers.apply(
        dm.ContainerApply(
            name=f"container_test_resource_loaders_{RUN_UNIQUE_ID}",
            space=schema_space.space,
            external_id=f"container_test_resource_loaders_{RUN_UNIQUE_ID}",
            properties={"name": dm.ContainerProperty(type=dm.Text())},
        )
    )


@pytest.fixture(scope="module")
def two_views(toolkit_client: ToolkitClient, schema_space: dm.Space, a_container: dm.Container) -> dm.ViewList:
    return toolkit_client.data_modeling.views.apply(
        [
            dm.ViewApply(
                space=schema_space.space,
                external_id="first_view",
                version="1",
                properties={
                    "name": dm.MappedPropertyApply(container=a_container.as_id(), container_property_identifier="name")
                },
            ),
            dm.ViewApply(
                space=schema_space.space,
                external_id="second_view",
                version="1",
                properties={
                    "alsoName": dm.MappedPropertyApply(
                        container=a_container.as_id(), container_property_identifier="name", name="name2"
                    )
                },
            ),
        ]
    )


class TestDataModelLoader:
    def test_create_update_delete(
        self, toolkit_client: ToolkitClient, schema_space: dm.Space, two_views: dm.ViewList
    ) -> None:
        loader = DataModelLoader(toolkit_client, None)
        view_list = two_views.as_ids()
        assert len(view_list) == 2, "Expected 2 views in the test data model"
        my_model = dm.DataModelApply(
            name="My model",
            description="Original description",
            views=view_list,
            space=schema_space.space,
            external_id=f"tmp_test_create_update_delete_data_model_{RUN_UNIQUE_ID}",
            version="1",
        )

        try:
            created = loader.create(dm.DataModelApplyList([my_model]))
            assert len(created) == 1

            update = dm.DataModelApply.load(my_model.dump())
            update.views = [view_list[0]]

            with pytest.raises(CogniteAPIError):
                loader.update(dm.DataModelApplyList([update]))
            # You need to update the version to update the model
            update.version = "2"

            updated = loader.update(dm.DataModelApplyList([update]))
            assert len(updated) == 1
            assert updated[0].views == [view_list[0]]
        finally:
            loader.delete([my_model.as_id()])


@pytest.fixture
def custom_file_container(toolkit_client: ToolkitClient, schema_space: dm.Space) -> dm.Container:
    return toolkit_client.data_modeling.containers.apply(
        dm.ContainerApply(
            name=f"container_test_resource_loaders_{RUN_UNIQUE_ID}",
            space=schema_space.space,
            external_id=f"container_test_resource_loaders_{RUN_UNIQUE_ID}",
            properties={
                "status": dm.ContainerProperty(type=dm.Text()),
                "fileCategory": dm.ContainerProperty(type=dm.Text()),
            },
        )
    )


@pytest.fixture
def cognite_file_extension(
    toolkit_client: ToolkitClient, custom_file_container: dm.Container, schema_space: dm.Space
) -> dm.View:
    container = custom_file_container.as_id()
    return toolkit_client.data_modeling.views.apply(
        dm.ViewApply(
            space=schema_space.space,
            external_id="CogniteFileExtension",
            version="v1",
            implements=[CogniteFileApply.get_source()],
            properties={
                "status": dm.MappedPropertyApply(container=container, container_property_identifier="status"),
                "fileCategory": dm.MappedPropertyApply(
                    container=container, container_property_identifier="fileCategory"
                ),
            },
        )
    )


class TestCogniteFileLoader:
    def test_create_update_retrieve_delete(self, toolkit_client: ToolkitClient, instance_space: dm.Space) -> None:
        loader = CogniteFileLoader(toolkit_client, None)
        # Loading from YAML to test the loading of extra properties as well
        file = ExtendableCogniteFileApply.load(f"""space: {instance_space.space}
externalId: tmp_test_create_update_delete_file_{RUN_UNIQUE_ID}
name: My file
description: Original description
""")
        try:
            created = loader.create(ExtendableCogniteFileApplyList([file]))
            assert len(created) == 1

            retrieved = loader.retrieve([file.as_id()])
            assert len(retrieved) == 1
            assert retrieved[0].name == "My file"
            assert retrieved[0].description == "Original description"

            update = ExtendableCogniteFileApply._load(file.dump(context="local"))
            update.description = "Updated description"

            updated = loader.update(ExtendableCogniteFileApplyList([update]))
            assert len(updated) == 1

            retrieved = loader.retrieve([file.as_id()])
            assert len(retrieved) == 1
            assert retrieved[0].description == "Updated description"
            assert retrieved[0].name == "My file"
        finally:
            loader.delete([file.as_id()])

    @pytest.mark.skip("For now, we do not support creating extensions")
    def test_create_update_retrieve_delete_extension(
        self, toolkit_client: ToolkitClient, cognite_file_extension: dm.View, instance_space: dm.Space
    ) -> None:
        loader = CogniteFileLoader(toolkit_client, None)
        view_id = cognite_file_extension.as_id()
        # Loading from YAML to test the loading of extra properties as well
        file = ExtendableCogniteFileApply.load(f"""space: {instance_space.space}
externalId: tmp_test_create_update_delete_file_extension_{RUN_UNIQUE_ID}
name: MyExtendedFile
description: Original description
nodeSource:
  space: {view_id.space}
  externalId: {view_id.external_id}
  version: {view_id.version}
  type: view
status: Active
fileCategory: Document
""")
        try:
            created = loader.create(ExtendableCogniteFileApplyList([file]))
            assert len(created) == 1

            update = ExtendableCogniteFileApply._load(file.dump(context="local"))
            # Ensure serialization and deserialization works
            assert update.name == "MyExtendedFile"
            assert update.extra_properties is not None
            assert update.extra_properties["fileCategory"] == "Document"
            update.extra_properties["status"] = "Inactive"

            updated = loader.update(ExtendableCogniteFileApplyList([update]))
            assert len(updated) == 1

            retrieved = loader.retrieve([file.as_id()])
            assert len(retrieved) == 1
            assert retrieved[0].name == "MyExtendedFile"
            assert retrieved[0].extra_properties is not None
            assert retrieved[0].extra_properties["status"] == "Inactive"
        finally:
            loader.delete([file.as_id()])


class TestGroupLoader:
    def test_dump_cdf_group_with_invalid_reference(self, toolkit_client: ToolkitClient) -> None:
        to_delete = TimeSeriesWrite(
            external_id="test_dump_cdf_group_with_invalid_reference",
            name="Test dump CDF group with invalid reference",
            is_step=False,
            is_string=False,
        )
        group_id: int | None = None
        try:
            created_ts = toolkit_client.time_series.create(to_delete)
            group = GroupWrite(
                name="test_dump_cdf_group_with_invalid_reference",
                source_id="1234-dummy",
                capabilities=[
                    TimeSeriesAcl(actions=[TimeSeriesAcl.Action.Read], scope=IDScopeLowerCase([created_ts.id]))
                ],
            )
            created_group = toolkit_client.iam.groups.create(group)
            group_id = created_group.id
            toolkit_client.time_series.delete(id=created_ts.id)

            loader = GroupLoader.create_loader(toolkit_client)

            dumped = loader.dump_resource(created_group)
            assert "capabilities" in dumped
            capabilities = dumped["capabilities"]
            assert isinstance(capabilities, list)
            assert len(capabilities) == 1
            assert capabilities[0] == {"timeSeriesAcl": {"actions": ["READ"], "scope": {"idscope": {"ids": []}}}}
        finally:
            toolkit_client.time_series.delete(external_id=to_delete.external_id, ignore_unknown_ids=True)
            if group_id:
                with suppress(CogniteAPIError):
                    toolkit_client.iam.groups.delete(id=group_id)


class TestWorkflowVersionLoader:
    def test_load_task_with_reference(self) -> None:
        definition_yaml = """workflowExternalId: myWorkflow
version: v1
workflowDefinition:
  description: Two tasks with reference
  tasks:
  - externalId: myTask2
    type: function
    parameters:
      function:
        externalId: fn_first_function
  - externalId: myTask2
    type: function
    parameters:
      function:
        externalId: ${myTask1.output.nextFunction}
        data: ${myTask1.output.data}
    dependsOn:
    - externalId: myTask1
"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = definition_yaml
        with monkeypatch_toolkit_client() as client:
            loader = WorkflowVersionLoader(client, None, None)

            with catch_warnings(EnvironmentVariableMissingWarning) as warning_list:
                loaded = loader.load_resource_file(file, {"myTask1.output.data": "should-be-ignored"})
            definition = loader.load_resource(loaded[0])

        assert len(definition.workflow_definition.tasks) == 2
        task2 = definition.workflow_definition.tasks[1]
        parameters = task2.parameters
        assert isinstance(parameters, FunctionTaskParameters)
        assert parameters.data == "${myTask1.output.data}"
        assert len(warning_list) == 0, "We should not get a warning for using a reference in a task parameter"

    def test_load_workflow_without_defaults_not_redeployed(self, toolkit_client: ToolkitClient) -> None:
        definition_yaml = """workflowExternalId: testWorkflowWithoutDefaults
version: v1
workflowDefinition:
  description: Tasks without defaults
  tasks:
  - externalId: myTask1
    type: function
    parameters:
      function:
        externalId: fn_first_function
  - externalId: myTask2
    type: transformation
    parameters:
      transformation:
        externalId: some_transformation
"""
        loader = WorkflowVersionLoader.create_loader(toolkit_client)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = definition_yaml

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(resource_dict[0])
        assert isinstance(resource, WorkflowVersionUpsert)
        if not loader.retrieve([resource.as_id()]):
            _ = loader.create(WorkflowVersionUpsertList([resource]))

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}


class TestTransformationLoader:
    def test_create_transformation_auth_without_scope(self, toolkit_client: ToolkitClient) -> None:
        transformation_text = """externalId: transformation_without_scope
name: This is a test transformation
destination:
  type: assets
ignoreNullFields: true
isPublic: true
conflictMode: upsert
query: Select * from assets
# Reusing the credentials from the Toolkit principal
authentication:
  clientId: ${IDP_CLIENT_ID}
  clientSecret: ${IDP_CLIENT_SECRET}
"""
        loader = TransformationLoader.create_loader(toolkit_client)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = transformation_text

        loaded = loader.load_resource_file(filepath, dict(os.environ))
        assert len(loaded) == 1
        transformation = loader.load_resource(loaded[0])

        try:
            created = loader.create([transformation])
            assert len(created) == 1
        finally:
            toolkit_client.transformations.delete(external_id="transformation_without_scope", ignore_unknown_ids=True)


class TestNodeLoader:
    def test_update_existing_node(self, toolkit_client: ToolkitClient, instance_space: dm.Space) -> None:
        loader = NodeLoader(toolkit_client, None)
        view_id = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
        existing_node = dm.NodeApply(
            space=instance_space.space,
            external_id=f"toolkit_test_update_existing_node_{RUN_UNIQUE_ID}",
            sources=[
                dm.NodeOrEdgeData(
                    view_id,
                    {
                        "name": "existing name",
                        "description": "Existing description",
                    },
                )
            ],
        )
        updated_node = dm.NodeApply(
            space=existing_node.space,
            external_id=existing_node.external_id,
            sources=[
                dm.NodeOrEdgeData(
                    view_id,
                    {
                        "name": "updated name",
                        "aliases": ["alias1", "alias2"],
                    },
                )
            ],
        )
        try:
            created = loader.create(dm.NodeApplyList([existing_node]))
            assert len(created) == 1

            updated = loader.update(dm.NodeApplyList([updated_node]))
            assert len(updated) == 1

            retrieved = toolkit_client.data_modeling.instances.retrieve(existing_node.as_id(), sources=[view_id])
            assert len(retrieved.nodes) == 1
            node = retrieved.nodes[0]
            assert node.properties[view_id] == {
                "name": "updated name",  # Overwrite
                "description": "Existing description",  # Keep existing description
                "aliases": ["alias1", "alias2"],  # Add new aliases
            }
        finally:
            loader.delete([existing_node.as_id()])


class TestViewLoader:
    def test_no_implement_not_redeployed(
        self, toolkit_client: ToolkitClient, schema_space: dm.Space, a_container: dm.Container
    ) -> None:
        definition_yaml = f"""space: {schema_space.space}
externalId: ToolkitTestNoImplementsNotRedeployed
version: v1
implements: []
properties:
  name:
    container:
      space: {a_container.space}
      externalId: {a_container.external_id}
      type: container
    containerPropertyIdentifier: name
        """
        loader = ViewLoader.create_loader(toolkit_client)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = definition_yaml

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(resource_dict[0])
        assert isinstance(resource, ViewApply)
        if not loader.retrieve([resource.as_id()]):
            _ = loader.create(ViewApplyList([resource]))

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}
