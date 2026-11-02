from typing import Any

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.resource_ios._neat._reader import NeatTableReader
from cognite_toolkit._cdf_tk.yaml_classes import NeatFormat
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatTableSource


def _load_neat_yaml(data: dict[str, Any]) -> NeatYAML:
    loaded = TypeAdapter(NeatFormat).validate_python(data)
    assert isinstance(loaded, NeatYAML)
    return loaded


class TestNeatTableReader:
    def test_read_minimal_round_trip(self) -> None:
        neat_yaml = _load_neat_yaml(
            {
                "Metadata": {"space": "my_space", "external_id": "MyModel", "version": "v1"},
                "Properties": [
                    {
                        "View": "Equipment",
                        "View Property": "name",
                        "Value Type": "text",
                        "Min Count": 0,
                        "Max Count": 1,
                        "Container": "Equipment",
                        "Container Property": "name",
                    }
                ],
                "Views": [{"View": "Equipment", "Filter": "hasData(Equipment)"}],
                "Containers": [{"Container": "Equipment", "Used For": "node"}],
            }
        )
        reader = NeatTableReader(neat_yaml, NeatTableSource(source="test.yaml"))

        result = reader.read()

        assert reader.get_warnings() == []
        assert result.space.identifier == SpaceId(space="my_space")
        assert result.space.raw == {"space": "my_space"}

        assert len(result.containers) == 1
        container = result.containers[0]
        assert container.identifier == ContainerId(space="my_space", external_id="Equipment")
        assert container.raw["properties"] == {
            "name": {"type": {"type": "text", "list": False}, "nullable": True},
        }

        assert len(result.views) == 1
        view = result.views[0]
        assert view.identifier == ViewId(space="my_space", external_id="Equipment", version="v1")
        assert view.raw["filter"] == {
            "hasData": [{"type": "container", "space": "my_space", "externalId": "Equipment"}]
        }
        assert view.raw["properties"] == {
            "name": {
                "container": {"type": "container", "space": "my_space", "externalId": "Equipment"},
                "containerPropertyIdentifier": "name",
            }
        }

        assert result.node_types == []

        assert result.data_model.identifier == DataModelId(space="my_space", external_id="MyModel", version="v1")
        assert result.data_model.raw["views"] == [
            {"type": "view", "space": "my_space", "externalId": "Equipment", "version": "v1"}
        ]

    def test_read_missing_referenced_container_warns(self) -> None:
        neat_yaml = _load_neat_yaml(
            {
                "Metadata": {"space": "my_space", "external_id": "MyModel", "version": "v1"},
                "Properties": [
                    {
                        "View": "Equipment",
                        "View Property": "name",
                        "Value Type": "text",
                        "Min Count": 0,
                        "Max Count": 1,
                        "Container": "Missing",
                        "Container Property": "name",
                    }
                ],
                "Views": [{"View": "Equipment"}],
                "Containers": [{"Container": "Equipment"}],
            }
        )
        reader = NeatTableReader(neat_yaml, NeatTableSource(source="test.yaml"))

        result = reader.read()

        warnings = reader.get_warnings()
        assert len(warnings) == 1
        assert warnings[0].code == "MISSING-REFERENCED-RESOURCE"
        assert "Missing" in warnings[0].message
        assert "row 1" in warnings[0].message

        assert len(result.containers) == 1
        assert result.containers[0].raw["properties"] == {}
