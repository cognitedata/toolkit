import json
from typing import Any, Literal

import pytest
import respx
from httpx2 import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    WorkflowExecutionFilter,
    WorkflowFilter,
    WorkflowFilterItem,
)


class TestWorkflowListFilters:
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "api_name, filter_, expected_filter",
        [
            pytest.param(
                "versions",
                WorkflowFilter(
                    workflow_filters=[
                        WorkflowFilterItem(external_id="wf_a"),
                        WorkflowFilterItem(external_id="wf_b", version="v2"),
                    ]
                ),
                {"workflowFilters": [{"externalId": "wf_a"}, {"externalId": "wf_b", "version": "v2"}]},
                id="versions by workflow and version",
            ),
            pytest.param(
                "executions",
                WorkflowExecutionFilter(
                    workflow_filters=[WorkflowFilterItem(external_id="wf_a")],
                    created_time_start=10,
                    status=["RUNNING"],
                ),
                {"workflowFilters": [{"externalId": "wf_a"}], "createdTimeStart": 10, "status": ["RUNNING"]},
                id="executions of all versions of a workflow",
            ),
        ],
    )
    def test_list_sends_filter(
        self,
        api_name: Literal["versions", "executions"],
        filter_: WorkflowFilter,
        expected_filter: dict[str, Any],
        respx_mock: respx.MockRouter,
        toolkit_config: ToolkitClientConfig,
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
        respx_mock.post(toolkit_config.create_api_url(f"/workflows/{api_name}/list")).mock(
            return_value=Response(status_code=200, json={"items": []})
        )

        getattr(client.workflows, api_name).list(filter=filter_, limit=10)

        assert json.loads(respx_mock.calls[0].request.content) == {"filter": expected_filter, "limit": 10}
