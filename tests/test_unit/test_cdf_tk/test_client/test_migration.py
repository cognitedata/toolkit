from typing import Any, ClassVar

import pytest
import responses
from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource


@pytest.fixture()
def lookup_client(
    toolkit_config: ToolkitClientConfig,
    rsps: responses.RequestsMock,
) -> tuple[ToolkitClient, responses.RequestsMock]:
    config = toolkit_config
    rsps.add(
        method=responses.POST,
        url=config.create_api_url("models/instances/query"),
        json=TestMigrationLookup.QUERY_RESPONSE,
        status=200,
    )

    return ToolkitClient(config=config), rsps


@pytest.mark.usefixtures("disable_pypi_check")
class TestMigrationLookup:
    SPACE = "my_space"
    EXISTING_ID = 123
    EXISTING_EXTERNAL_ID = "node_123"
    EXISTING_NODE_ID = NodeId(SPACE, "node_123")
    QUERY_RESPONSE: ClassVar[dict[str, Any]] = {
        "items": {
            "instanceSource": [
                InstanceSource(
                    space=EXISTING_NODE_ID.space,
                    external_id=EXISTING_NODE_ID.external_id,
                    version=1,
                    last_updated_time=1,
                    created_time=1,
                    resource_type="asset",
                    id_=EXISTING_ID,
                    classic_external_id=EXISTING_EXTERNAL_ID,
                ).dump()
            ]
        },
        "nextCursor": {"instanceSource": None},
    }

    @pytest.mark.parametrize(
        "args, expected_return",
        [
            pytest.param({"id": EXISTING_ID}, EXISTING_NODE_ID, id="Exiting single ID"),
            pytest.param({"external_id": EXISTING_EXTERNAL_ID}, EXISTING_NODE_ID, id="Existing single external ID"),
            pytest.param({"id": -1}, None, id="Non-existing single ID"),
            pytest.param({"external_id": "non_existing_external_id"}, None, id="Non-existing single external ID"),
            pytest.param(
                {"id": [EXISTING_ID, -1]}, {EXISTING_ID: EXISTING_NODE_ID}, id="Mixed existing and non-existing IDs"
            ),
            pytest.param(
                {"external_id": [EXISTING_EXTERNAL_ID, "non_existing_external_id"]},
                {EXISTING_EXTERNAL_ID: EXISTING_NODE_ID},
                id="Mixed existing and non-existing external IDs",
            ),
        ],
    )
    def test_return_type_given_input(
        self,
        args: dict[str, Any],
        expected_return: dict | NodeId | None,
        lookup_client: tuple[ToolkitClient, responses.RequestsMock],
    ) -> None:
        client, _ = lookup_client
        actual_return = client.migration.lookup.assets(**args)
        assert actual_return == expected_return

    def test_multi_lookup_single_api_call(self, lookup_client: tuple[ToolkitClient, responses.RequestsMock]) -> None:
        client, rsps = lookup_client
        _ = client.migration.lookup.assets(self.EXISTING_ID)
        _ = client.migration.lookup.assets(self.EXISTING_ID)

        assert len(rsps.calls) == 1, "Expected only one API call for multiple lookups of the same ID"

    def test_single_api_call_non_existing(self, lookup_client: tuple[ToolkitClient, responses.RequestsMock]) -> None:
        client, rsps = lookup_client
        _ = client.migration.lookup.assets(-1)
        _ = client.migration.lookup.assets(-1)

        assert len(rsps.calls) == 1, "Expected only one API call for multiple lookups of the same non-existing ID"
