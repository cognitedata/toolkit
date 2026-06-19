from unittest.mock import MagicMock

import pytest
import typer

from cognite_toolkit._cdf_tk.apps._migrate_app import (
    _node_external_id_in_filter,
    _resolve_image360_collections,
    _resolve_image360_station_ids,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId, NodeResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.image360_data_mappings import IMAGE360_SOURCE_VIEW
from cognite_toolkit._cdf_tk.dataio import DataItem, Page
from cognite_toolkit._cdf_tk.dataio.progress import NoBookmark


class TestResolveImage360Collections:
    def test_collection_external_ids_with_instance_space(self) -> None:
        with monkeypatch_toolkit_client() as client:
            result = _resolve_image360_collections(
                client,
                "migrate",
                ["collection_a", "collection_b"],
                "my_space",
            )

        assert result == NodeId.from_str_ids(["collection_a", "collection_b"], space="my_space")

    def test_collection_requires_instance_space(self) -> None:
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(typer.BadParameter, match="must be provided together"):
                _resolve_image360_collections(client, "migrate", ["collection_a"], None)

    def test_instance_space_requires_collection(self) -> None:
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(typer.BadParameter, match="must be provided together"):
                _resolve_image360_collections(client, "migrate", None, "my_space")


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
                        "station": NodeId(space="my_space", external_id="station_a"),
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
                        "station": NodeId(space="my_space", external_id="station_b"),
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
                        "station": NodeId(space="my_space", external_id="station_c"),
                    }
                },
            ),
        ]

    def test_resolve_station_ids_for_selected_collection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        filtered_nodes = [self._image360_nodes()[0], self._image360_nodes()[2]]
        mock_io = MagicMock()
        mock_io.stream_data.return_value = [
            Page(
                worker_id="main",
                items=[DataItem(tracking_id=f"{node.space}:{node.external_id}", item=node) for node in filtered_nodes],
                bookmark=NoBookmark(),
            )
        ]
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._migrate_app.InstanceIO",
            lambda client: mock_io,
        )

        with monkeypatch_toolkit_client() as client:
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

        assert _node_external_id_in_filter(station_ids) == {
            "in": {"property": ["node", "externalId"], "values": ["station_a", "station_c"]}
        }
