from cognite.client.data_classes import Function, FunctionSchedule, FunctionScheduleWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient


class TestExtendedFunctionScheduleAPI:
    def test_create_function_schedule(self, toolkit_client: ToolkitClient, dummy_function: Function) -> None:
        """Test creating a function schedule with a nonce."""
        session = toolkit_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        schedule = FunctionScheduleWrite(
            name="toolkit_test_create_schedule",
            cron_expression="0 0 * * *",
            function_id=dummy_function.id,
            description="This is a test schedule",
        )
        created: FunctionSchedule | None = None
        try:
            created = toolkit_client.functions.schedules.create_api(schedule, session.nonce)

            assert created.as_write().dump() == schedule.dump()
        finally:
            if created:
                toolkit_client.functions.schedules.delete(created.id)
