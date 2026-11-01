import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._table import EntityTableFilter


def _table(**overrides: object) -> dict[str, object]:
    table: dict[str, object] = {
        "Metadata": [
            {"Key": "space", "Value": "test"},
            {"Key": "externalId", "Value": "assetModel"},
            {"Key": "version", "Value": "v1"},
        ],
        "Properties": [
            {
                "View": "Asset",
                "View Property": "name",
                "Connection": None,
                "Value Type": "text(maxTextSize=400)",
                "Min Count": 0,
                "Max Count": 1,
            }
        ],
        "Views": [{"View": "Asset", "Filter": "hasData(test:Asset)"}],
        "Containers": [{"Container": "Asset", "Used For": "node"}],
    }
    table.update(overrides)
    return table


def test_neat_yaml_validates_table_rows() -> None:
    resource = NeatYAML.model_validate(_table())

    assert (
        resource.as_id(),
        resource.properties[0].view.suffix,
        resource.properties[0].value_type.properties,
        resource.views[0].filter,
    ) == (
        DataModelId(space="test", external_id="assetModel", version="v1"),
        "Asset",
        {"maxTextSize": "400"},
        EntityTableFilter.model_validate({"type": "hasData", "entities": "test:Asset"}),
    )


def test_neat_yaml_rejects_invalid_rows() -> None:
    with pytest.raises(ValidationError) as error:
        NeatYAML.model_validate(
            _table(
                Properties=[
                    {
                        "View": "asset:MyAsset(capacity=100,type=storage)trailing",
                        "View Property": "name",
                        "Value Type": "text",
                        "Min Count": "not_an_int",
                        "Max Count": 1,
                    }
                ]
            )
        )

    messages = [item["msg"] for item in error.value.errors()]
    assert (
        any("Invalid entity syntax" in message for message in messages),
        any("valid integer" in message for message in messages),
    ) == (True, True)
