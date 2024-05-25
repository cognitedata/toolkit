from asyncio import sleep

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionSchedule, FunctionScheduleWriteList

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.load import DataSetsLoader, FunctionScheduleLoader


class TestDataSetsLoader:
    def test_existing_unchanged(self, cognite_client: CogniteClient):
        data_sets = cognite_client.data_sets.list(limit=1, external_id_prefix="")
        loader = DataSetsLoader(client=cognite_client)

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
        loader = FunctionScheduleLoader(client=cognite_client)
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
            # so we wait a bit and try again.
            sleep(1)
            retrieved = loader.retrieve([identifier])

        assert retrieved[0].description == function_schedule.description
