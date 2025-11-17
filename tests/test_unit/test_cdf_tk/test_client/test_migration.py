from typing import Any

import pytest
import responses
from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource


class TestMigrationLookup:
    SPACE = "my_space"
    EXISTING_ID = 123
    EXISTING_EXTERNAL_ID = "node_123"
    EXISTING_NODE_ID = NodeId(SPACE, "node_123")

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
        toolkit_config: ToolkitClientConfig,
        rsps: responses.RequestsMock,
    ) -> None:
        config = toolkit_config
        rsps.add(
            method=responses.POST,
            url=config.create_api_url("models/instances/query"),
            json={
                "items": {
                    "instanceSource": [
                        InstanceSource(
                            space=self.EXISTING_NODE_ID.space,
                            external_id=self.EXISTING_NODE_ID.external_id,
                            version=1,
                            last_updated_time=1,
                            created_time=1,
                            resource_type="asset",
                            id_=self.EXISTING_ID,
                            classic_external_id=self.EXISTING_EXTERNAL_ID,
                        ).dump()
                    ]
                },
                "nextCursor": {"instanceSource": None},
            },
            status=200,
        )

        client = ToolkitClient(config=config)

        actual_return = client.migration.lookup.assets(**args)
        assert actual_return == expected_return
