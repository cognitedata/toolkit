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
    FunctionWrite,
    FunctionWriteList,
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
from cognite.client.data_classes.data_modeling import NodeApplyList, NodeList, Space, ViewApply, ViewApplyList
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply, CogniteTimeSeries, CogniteTimeSeriesApply
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
from cognite_toolkit._cdf_tk.cruds import (
    AssetCRUD,
    CogniteFileCRUD,
    DataModelCRUD,
    DatapointSubscriptionCRUD,
    FunctionCRUD,
    FunctionScheduleCRUD,
    GroupCRUD,
    LabelCRUD,
    NodeCRUD,
    ResourceWorker,
    RobotCapabilityCRUD,
    RoboticsDataPostProcessingCRUD,
    TransformationCRUD,
    ViewCRUD,
    WorkflowVersionCRUD,
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
        loader = FunctionScheduleCRUD(toolkit_client, None, None)
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
        loader = FunctionScheduleCRUD(toolkit_client, None, None)
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


@pytest.fixture(scope="session")
def one_hundred_and_one_timeseries(cognite_client: CogniteClient, toolkit_dataset: DataSet) -> TimeSeriesList:
    ts_list = TimeSeriesWriteList(
        [
            TimeSeriesWrite(
                external_id=f"toolkit_101_timeseries_{i}",
                name=f"Toolkit Test 101 TimeSeries {i}",
                is_step=False,
                is_string=False,
                data_set_id=toolkit_dataset.id,
            )
            for i in range(1, 102)
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


@pytest.fixture(scope="session")
def three_hundred_and_three_cognite_timeseries(
    toolkit_client: ToolkitClient, toolkit_space: Space
) -> NodeList[CogniteTimeSeries]:
    ts_list = NodeApplyList(
        [
            CogniteTimeSeriesApply(
                space=toolkit_space.space,
                external_id=f"toolkit_303_test_timeseries_{i}",
                is_step=False,
                time_series_type="numeric",
            )
            for i in range(1, 304)
        ]
    )
    retrieved = toolkit_client.data_modeling.instances.retrieve_nodes(ts_list.as_ids(), node_cls=CogniteTimeSeries)
    if len(retrieved) == len(ts_list):
        # All timeseries already exist, return the retrieved ones
        return retrieved
    existing = set(retrieved.as_ids())
    to_create = [ts for ts in ts_list if ts.as_id() not in existing]
    if to_create:
        _ = toolkit_client.data_modeling.instances.apply(to_create)
    return toolkit_client.data_modeling.instances.retrieve_nodes(ts_list.as_ids(), node_cls=CogniteTimeSeries)


class TestDatapointSubscriptionLoader:
    def test_delete_non_existing(self, toolkit_client: ToolkitClient) -> None:
        loader = DatapointSubscriptionCRUD(toolkit_client, None)
        delete_count = loader.delete(["non_existing"])
        assert delete_count == 0

    def test_create_update_delete_subscription(self, toolkit_client: ToolkitClient) -> None:
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

        loader = DatapointSubscriptionCRUD(toolkit_client, None)

        try:
            created = loader.create(DatapointSubscriptionWriteList([sub]))
            assert len(created) == 1

            updated = loader.update(DatapointSubscriptionWriteList([update]))
            assert len(updated) == 1
            assert updated[0].name == "Updated name"
        finally:
            loader.delete([sub.external_id])

    def test_create_update_delete_subscription_with_ids(
        self,
        toolkit_client: ToolkitClient,
        one_hundred_and_one_timeseries: TimeSeriesList,
        three_timeseries: TimeSeriesList,
        three_hundred_and_three_cognite_timeseries: NodeList[CogniteTimeSeries],
    ) -> None:
        ts_ids = "\n- ".join(one_hundred_and_one_timeseries.as_external_ids())
        cognite_ts_ids = "\n".join(
            [
                f"- space: {node.space}\n  externalId: {node.external_id}"
                for node in three_hundred_and_three_cognite_timeseries
            ]
        )
        sub_yaml = f"""externalId: tmp_test_create_update_delete_101_subscription_{RUN_UNIQUE_ID}
partitionCount: 1
name: The subscription name
timeSeriesIds:
- {ts_ids}
instanceIds:
{cognite_ts_ids}
"""
        ts_update_ds = "\n- ".join(
            one_hundred_and_one_timeseries.as_external_ids() + three_timeseries.as_external_ids()
        )
        update_yaml = f"""externalId: tmp_test_create_update_delete_101_subscription_{RUN_UNIQUE_ID}
partitionCount: 1
name: The subscription name
timeSeriesIds:
- {ts_update_ds}
"""
        loader = DatapointSubscriptionCRUD(toolkit_client, None)
        sub = self._load_subscription_from_yaml(self._create_mock_file(sub_yaml), loader)
        try:
            created = loader.create(DatapointSubscriptionWriteList([sub]))
            assert len(created) == 1
            initial_description = created[0].description
            assert created[0].time_series_count == len(one_hundred_and_one_timeseries) + len(
                three_hundred_and_three_cognite_timeseries
            ), "The subscription should have the correct number of time series"

            update = self._load_subscription_from_yaml(self._create_mock_file(update_yaml), loader)
            updated = loader.update(DatapointSubscriptionWriteList([update]))
            assert len(updated) == 1
            updated_description = updated[0].description
            assert updated_description != initial_description, (
                "The description should have changed after the update with a hash fo the timeseries IDs"
            )
            assert updated[0].time_series_count == len(one_hundred_and_one_timeseries) + len(three_timeseries), (
                "The subscription should have the correct number of time series after the update"
            )
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
        loader = DatapointSubscriptionCRUD.create_loader(toolkit_client)

        filepath = self._create_mock_file(definition_yaml)
        resource = self._load_subscription_from_yaml(filepath, loader)
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

    @staticmethod
    def _create_mock_file(yaml_content: str) -> Path:
        mock_file = MagicMock(spec=Path)
        mock_file.read_text.return_value = yaml_content
        return mock_file

    @staticmethod
    def _load_subscription_from_yaml(filepath: Path, loader: DatapointSubscriptionCRUD) -> DataPointSubscriptionWrite:
        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        return loader.load_resource(resource_dict[0])


class TestLabelLoader:
    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        loader = LabelCRUD(cognite_client, None)
        delete_count = loader.delete(["non_existing"])
        assert delete_count == 0

    def test_create_delete_label(self, toolkit_client: ToolkitClient) -> None:
        label = LabelDefinitionWrite(
            external_id=f"tmp_test_create_update_delete_label_{RUN_UNIQUE_ID}",
            name="Initial name",
        )
        loader = LabelCRUD(toolkit_client, None)

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

        loader = AssetCRUD(cognite_client, None)

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
        loader = RobotCapabilityCRUD(toolkit_client, None)

        capabilities = loader.retrieve([existing_robot_capability.external_id, "non_existing_robot"])

        assert len(capabilities) == 1

    def test_create_update_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        loader = RobotCapabilityCRUD(toolkit_client, None)

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
        loader = RoboticsDataPostProcessingCRUD(toolkit_client, None)

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
        loader = DataModelCRUD(toolkit_client, None)
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
        loader = CogniteFileCRUD(toolkit_client, None)
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
        loader = CogniteFileCRUD(toolkit_client, None)
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

            loader = GroupCRUD.create_loader(toolkit_client)

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
            loader = WorkflowVersionCRUD(client, None, None)

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
    retries: null
"""
        loader = WorkflowVersionCRUD.create_loader(toolkit_client)

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
        loader = TransformationCRUD.create_loader(toolkit_client)
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

    def test_create_transformation_reusing_source_destination_auth(self, toolkit_client: ToolkitClient) -> None:
        transformation_text = """externalId: transformation_reusing_source_destination_auth
name: This is a test transformation from the Toolkit
destination:
  type: assets
ignoreNullFields: true
isPublic: true
conflictMode: upsert
query: Select * from assets
# Reusing the credentials from the Toolkit principal
authentication:
  read:
    clientId: ${IDP_CLIENT_ID}
    clientSecret: ${IDP_CLIENT_SECRET}
  write:
    clientId: ${IDP_CLIENT_ID}
    clientSecret: ${IDP_CLIENT_SECRET}
        """
        loader = TransformationCRUD.create_loader(toolkit_client)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = transformation_text

        loaded = loader.load_resource_file(filepath, dict(os.environ))
        assert len(loaded) == 1
        transformation = loader.load_resource(loaded[0])

        try:
            created_list = loader.create([transformation])
            assert len(created_list) == 1
            created = created_list[0]
            assert created.source_session is not None
            assert created.destination_session is not None
            assert created.source_session.session_id != created.destination_session.session_id, (
                "There should be different sessions for source and destination authentication even"
                " if they reuse the same credentials"
            )
        finally:
            toolkit_client.transformations.delete(
                external_id="transformation_reusing_source_destination_auth", ignore_unknown_ids=True
            )

    @pytest.mark.skip(
        reason="This is a load test that takes a long time to run ~5 minutes, "
        "and puts a high load on the transformation service. "
        "It is used to verify that a fix works, but not run regularly."
    )
    def test_load_test_transformation_creation(self, toolkit_client: ToolkitClient) -> None:
        credentials = toolkit_client.config.credentials
        if not isinstance(credentials, OAuthClientCredentials):
            pytest.skip("This test requires OAuthClientCredentials to run")
        secret = credentials.client_secret
        client_id = credentials.client_id
        N = 500
        definition_yaml = [
            f"""- externalId: test_load_transformation_creation_{RUN_UNIQUE_ID}_{i}
  name: Load Test Transformation Creation {i}
  destination:
    type: assets
  ignoreNullFields: true
  isPublic: true
  conflictMode: upsert
  query: Select * from assets
  authentication:
    clientId: {client_id}
    clientSecret: {secret}
"""
            for i in range(1, N + 1)
        ]

        loader = TransformationCRUD.create_loader(toolkit_client)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = "\n".join(definition_yaml)

        loaded = loader.load_resource_file(filepath, {})
        assert len(loaded) == N
        transformations = [loader.load_resource(resource) for resource in loaded]

        try:
            created_list = loader.create(transformations)
            assert len(created_list) == N
        finally:
            loader.delete([transformation.external_id for transformation in transformations])


class TestNodeLoader:
    def test_update_existing_node(self, toolkit_client: ToolkitClient, instance_space: dm.Space) -> None:
        loader = NodeCRUD(toolkit_client, None)
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
        loader = ViewCRUD.create_loader(toolkit_client)

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


class TestFunctionLoader:
    def test_avoid_redeploying_function_with_no_changes(
        self, toolkit_client: ToolkitClient, toolkit_dataset: DataSet, tmp_path: Path
    ) -> None:
        function_code = """from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    # This will fail unless the function has the specified capabilities.
    print("Print statements will be shown in the logs.")
    print("Running with the following configuration:\n")
    return {
        "data": data,
        "functionInfo": function_call_info,
    }

"""
        external_id = "toolkit_test_function_no_redeploy"
        definition_yaml = f"""externalId: {external_id}
name: Toolkit Test Function No Redeploy
owner: ""
dataSetExternalId: {toolkit_dataset.external_id}
description: ""
        """
        build_dir = tmp_path / "build"
        function_code_path = build_dir / FunctionCRUD.folder_name / external_id / "handler.py"
        function_code_path.parent.mkdir(parents=True, exist_ok=True)
        function_code_path.write_text(function_code, encoding="utf-8")

        loader = FunctionCRUD.create_loader(toolkit_client, build_dir)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = definition_yaml
        filepath.parent.name = FunctionCRUD.folder_name
        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(resource_dict[0])
        assert isinstance(resource, FunctionWrite)
        if not loader.retrieve([resource.external_id]):
            _ = loader.create(FunctionWriteList([resource]))
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}
