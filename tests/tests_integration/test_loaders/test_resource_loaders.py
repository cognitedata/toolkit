from asyncio import sleep

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    DataPointSubscriptionWrite,
    Function,
    FunctionSchedule,
    FunctionScheduleWriteList,
    LabelDefinitionWrite,
    filters,
)
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscriptionProperty,
    DatapointSubscriptionWriteList,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import DataSetsLoader, FunctionScheduleLoader, LabelLoader
from cognite_toolkit._cdf_tk.loaders._resource_loaders import DatapointSubscriptionLoader
from tests.tests_integration.constants import RUN_UNIQUE_ID


class TestDataSetsLoader:
    def test_existing_unchanged(self, cognite_client: CogniteClient):
        data_sets = cognite_client.data_sets.list(limit=1, external_id_prefix="")
        loader = DataSetsLoader(cognite_client, None)

        cmd = DeployCommand(print_warning=False)
        created, changed, unchanged = cmd.to_create_changed_unchanged_triple(data_sets.as_write(), loader)

        assert len(unchanged) == len(data_sets)
        assert len(created) == 0
        assert len(changed) == 0


@pytest.fixture
def dummy_function(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test_function_dummy"

    if existing := cognite_client.functions.retrieve(external_id=external_id):
        return existing

    def handle(client: CogniteClient, data: dict, function_call_info: dict) -> str:
        """
        [requirements]
        cognite-sdk>=7.37.0
        [/requirements]
        """
        print("Print statements will be shown in the logs.")
        print("Running with the following configuration:\n")
        return {
            "data": data,
            "functionInfo": function_call_info,
        }

    return cognite_client.functions.create(
        name="integration_test_function_dummy",
        function_handle=handle,
        external_id="integration_test_function_dummy",
    )


@pytest.fixture
def dummy_schedule(cognite_client: CogniteClient, dummy_function: Function) -> FunctionSchedule:
    name = "integration_test_schedule_dummy"
    if existing_list := cognite_client.functions.schedules.list(
        function_external_id=dummy_function.external_id, name=name
    ):
        created = existing_list[0]
    else:
        created = cognite_client.functions.schedules.create(
            name=name,
            cron_expression="0 7 * * MON",
            description="Original description.",
            function_external_id=dummy_function.external_id,
        )
    if created.function_external_id is None:
        created.function_external_id = dummy_function.external_id
    return created


class TestFunctionScheduleLoader:
    def test_update_function_schedule(
        self, cognite_client: CogniteClient, dummy_function: Function, dummy_schedule: FunctionSchedule
    ) -> None:
        loader = FunctionScheduleLoader(cognite_client, None)
        function_schedule = dummy_schedule.as_write()

        function_schedule.description = (
            "Updated description."
            if function_schedule.description != "Updated description."
            else "Original description."
        )
        identifier = loader.get_id(function_schedule)

        loader.update(FunctionScheduleWriteList([function_schedule]))

        retrieved = loader.retrieve([identifier])
        if not retrieved or retrieved[0].description != function_schedule.description:
            # The service can be a bit slow in returning the updated description,
            # so we wait a bit and try again. (Eventual consistency)
            sleep(2)
            retrieved = loader.retrieve([identifier])

        assert retrieved, "Function schedule not found after update."
        assert retrieved[0].description == function_schedule.description


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


class TestLabelLoader:
    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        loader = LabelLoader(cognite_client, None)
        delete_count = loader.delete(["non_existing"])
        assert delete_count == 0

    def test_create_update_delete_label(self, cognite_client: CogniteClient) -> None:
        initial = LabelDefinitionWrite(
            external_id=f"tmp_test_create_update_delete_label_{RUN_UNIQUE_ID}",
            name="Initial name",
        )
        update = LabelDefinitionWrite(
            external_id=f"tmp_test_create_update_delete_label_{RUN_UNIQUE_ID}",
            name="Updated name",
        )

        loader = LabelLoader(cognite_client, None)

        try:
            created = loader.create(LabelDefinitionWriteList([initial]))
            assert len(created) == 1

            updated = loader.update(LabelDefinitionWriteList([update]))
            assert len(updated) == 1
            assert updated[0].name == "Updated name"
        finally:
            loader.delete([initial.external_id])
