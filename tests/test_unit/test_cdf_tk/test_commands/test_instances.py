from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.commands.instances import InstancesAPICommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def _node() -> NodeResponse:
    return NodeResponse(
        space="my_space",
        external_id="asset-1",
        version=1,
        created_time=1,
        last_updated_time=1,
    )


class TestInstancesCommand:
    def test_list_calls_search_endpoint(self) -> None:
        client = MagicMock()
        node = _node()
        client.tool.instances.search.return_value = [node]
        cmd = InstancesAPICommand(skip_tracking=True, silent=True)
        result = cmd.list(
            client,
            view="my_space:Asset/v1",
            filter='{"equals": {"property": ["node", "space"], "value": "my_space"}}',
            limit=10,
        )

        assert result == [node]
        client.tool.instances.search.assert_called_once_with(
            view=ViewId(space="my_space", external_id="Asset", version="v1"),
            filter={"equals": {"property": ["node", "space"], "value": "my_space"}},
            limit=10,
            instance_type="node",
        )

    def test_list_passes_instance_type(self) -> None:
        client = MagicMock()
        client.tool.instances.search.return_value = []
        cmd = InstancesAPICommand(skip_tracking=True, silent=True)
        result = cmd.list(client, view="my_space:Asset/v1", instance_type="edge")

        assert result == []
        client.tool.instances.search.assert_called_once_with(
            view=ViewId(space="my_space", external_id="Asset", version="v1"),
            filter=None,
            limit=25,
            instance_type="edge",
        )

    def test_list_invalid_view(self) -> None:
        cmd = InstancesAPICommand(skip_tracking=True, silent=True)
        with pytest.raises(ToolkitValueError, match="Invalid view string format"):
            cmd.list(MagicMock(), view="not-a-view")

    def test_list_invalid_filter(self) -> None:
        cmd = InstancesAPICommand(skip_tracking=True, silent=True)
        with pytest.raises(ToolkitValueError, match="Filter expression must be a YAML/JSON object"):
            cmd.list(MagicMock(), view="my_space:Asset/v1", filter="[1, 2]")
