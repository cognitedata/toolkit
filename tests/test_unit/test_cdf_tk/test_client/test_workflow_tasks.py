import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestWorkflowTasksAPI:
    def test_update(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/workflows/tasks/task-1/update")).mock(
            return_value=httpx2.Response(200, json={"id": "task-1", "externalId": "my_task", "status": "COMPLETED"})
        )

        toolkit_client.workflows.tasks.update("task-1", "COMPLETED", output={"answer": 42})

        assert json.loads(route.calls[0].request.content) == {"status": "COMPLETED", "output": {"answer": 42}}
