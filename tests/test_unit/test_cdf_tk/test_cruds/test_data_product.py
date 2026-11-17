"""Unit tests for DataProductIO."""

from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewNoVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._data_product import DataProductIO
from cognite_toolkit._cdf_tk.resource_ios._datamodel import SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.yaml_classes import DataProductYAML

SCOPE_SOURCE = {"type": "view", "space": "sales_schema", "externalId": "Order"}


class TestDataProductInstanceScope:
    def test_get_dependencies_yields_scope_views_and_spaces(self) -> None:
        resource = DataProductYAML.model_validate(
            {
                "externalId": "sales",
                "name": "Sales",
                "instanceScope": [
                    {
                        "source": {"space": "sales_schema", "externalId": "Order"},
                        "read": ["sales_prod"],
                        "upsert": ["sales_eu"],
                    }
                ],
            }
        )

        actual = set(DataProductIO.get_dependencies(resource))

        assert actual == {
            (ViewIO, ViewNoVersionId(space="sales_schema", external_id="Order")),
            (SpaceIO, SpaceId(space="sales_schema")),
            (SpaceIO, SpaceId(space="sales_prod")),
            (SpaceIO, SpaceId(space="sales_eu")),
        }

    def test_as_update_sets_instance_scope(self) -> None:
        request = DataProductRequest.model_validate(
            {
                "externalId": "sales",
                "name": "Sales",
                "instanceScope": [{"source": SCOPE_SOURCE, "read": ["sales_prod"]}],
            }
        )

        update = request.as_update(mode="replace")

        assert update["update"]["instanceScope"] == {
            "set": [{"source": SCOPE_SOURCE, "read": ["sales_prod"], "upsert": [], "delete": []}]
        }

    def test_as_update_without_instance_scope_omits_it(self) -> None:
        request = DataProductRequest.model_validate({"externalId": "sales", "name": "Sales"})

        update = request.as_update(mode="replace")

        assert "instanceScope" not in update["update"]

    def test_dump_resource_strips_server_defaults(self) -> None:
        io = DataProductIO(ToolkitClientMock())
        resource = DataProductResponse.model_validate(
            {
                "externalId": "sales",
                "name": "Sales",
                "schemaSpace": "sales",
                "isGoverned": False,
                "tags": [],
                "domains": [],
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "instanceScope": [{"source": SCOPE_SOURCE, "read": ["sales_prod"], "upsert": [], "delete": []}],
            }
        )
        local = {
            "externalId": "sales",
            "name": "Sales",
            "instanceScope": [{"source": {"space": "sales_schema", "externalId": "Order"}, "read": ["sales_prod"]}],
        }

        dumped = io.dump_resource(resource, local)

        assert dumped["instanceScope"] == [
            {"source": {"space": "sales_schema", "externalId": "Order"}, "read": ["sales_prod"]}
        ]

    def test_dump_resource_drops_empty_instance_scope(self) -> None:
        io = DataProductIO(ToolkitClientMock())
        resource = DataProductResponse.model_validate(
            {
                "externalId": "sales",
                "name": "Sales",
                "schemaSpace": "sales",
                "isGoverned": False,
                "tags": [],
                "domains": [],
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "instanceScope": [],
            }
        )

        dumped = io.dump_resource(resource, {"externalId": "sales", "name": "Sales"})

        assert "instanceScope" not in dumped
