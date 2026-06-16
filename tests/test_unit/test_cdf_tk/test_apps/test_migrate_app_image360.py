from cognite_toolkit._cdf_tk.apps._migrate_app import (
    _image360_station_node_filter,
    _resolve_image360_station_ids,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId, NodeResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.image360_data_mappings import IMAGE360_SOURCE_VIEW


class TestResolveImage360StationIds:
    @staticmethod
    def _image360_nodes() -> list[NodeResponse]:
        return [
            NodeResponse(
                space="my_space",
                external_id="image_a",
                created_time=1,
                last_updated_time=1,
                version=1,
                properties={
                    IMAGE360_SOURCE_VIEW: {
                        "collection360": {"space": "my_space", "externalId": "collection_0"},
                        "station": {"space": "my_space", "externalId": "station_a"},
                    }
                },
            ),
            NodeResponse(
                space="my_space",
                external_id="image_b",
                created_time=1,
                last_updated_time=1,
                version=1,
                properties={
                    IMAGE360_SOURCE_VIEW: {
                        "collection360": {"space": "my_space", "externalId": "collection_1"},
                        "station": {"space": "my_space", "externalId": "station_b"},
                    }
                },
            ),
            NodeResponse(
                space="my_space",
                external_id="image_c",
                created_time=1,
                last_updated_time=1,
                version=1,
                properties={
                    IMAGE360_SOURCE_VIEW: {
                        "collection360": {"space": "my_space", "externalId": "collection_0"},
                        "station": {"space": "my_space", "externalId": "station_c"},
                    }
                },
            ),
        ]

    def test_resolve_station_ids_for_selected_collection(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.tool.instances.list.return_value = self._image360_nodes()
            result = _resolve_image360_station_ids(
                client,
                [NodeId(space="my_space", external_id="collection_0")],
            )

        assert result == [
            NodeId(space="my_space", external_id="station_a"),
            NodeId(space="my_space", external_id="station_c"),
        ]

    def test_station_node_filter(self) -> None:
        station_ids = [
            NodeId(space="my_space", external_id="station_a"),
            NodeId(space="my_space", external_id="station_c"),
        ]

        assert _image360_station_node_filter(station_ids) == {
            "in": {"property": ["node", "externalId"], "values": ["station_a", "station_c"]}
        }
