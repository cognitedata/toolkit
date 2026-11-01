from pathlib import Path

from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._neat import NeatIO
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML


def test_neat_table_is_converted_to_data_model_resources() -> None:
    raw = {
        "Metadata": [
            {"Key": "space", "Value": "test"},
            {"Key": "externalId", "Value": "assetModel"},
            {"Key": "version", "Value": "v1"},
        ],
        "Properties": [
            {
                "View": "Asset",
                "View Property": "name",
                "Value Type": "text(maxTextSize=400)",
                "Min Count": 0,
                "Max Count": 1,
                "Container": "Asset",
                "Container Property": "name",
                "Index": "btree:name(cursorable=True)",
            },
            {
                "View": "Asset",
                "View Property": "parent",
                "Connection": "direct",
                "Value Type": "Asset",
                "Min Count": 0,
                "Max Count": 1,
                "Container": "Asset",
                "Container Property": "parent",
            },
        ],
        "Views": [{"View": "Asset"}],
        "Containers": [{"Container": "Asset", "Used For": "node"}],
        "Nodes": [{"Node": "AssetType"}],
    }

    resources = [
        item
        for item in NeatIO.to_crud_types(raw, Path("asset.neat.yaml"), NeatYAML.model_validate(raw))
        if isinstance(item, CrudResource)
    ]

    assert [(resource.type_.kind, resource.raw) for resource in resources] == [
        ("Space", {"space": "test"}),
        (
            "Container",
            {
                "space": "test",
                "externalId": "Asset",
                "usedFor": "node",
                "properties": {
                    "name": {"nullable": True, "type": {"type": "text", "list": False, "maxTextSize": 400}},
                    "parent": {"nullable": True, "type": {"type": "direct", "list": False}},
                },
                "constraints": {},
                "indexes": {"name": {"indexType": "btree", "properties": ["name"], "cursorable": True}},
            },
        ),
        (
            "View",
            {
                "space": "test",
                "externalId": "Asset",
                "version": "v1",
                "implements": [],
                "properties": {
                    "name": {
                        "container": {"type": "container", "space": "test", "externalId": "Asset"},
                        "containerPropertyIdentifier": "name",
                    },
                    "parent": {
                        "container": {"type": "container", "space": "test", "externalId": "Asset"},
                        "containerPropertyIdentifier": "parent",
                        "source": {"type": "view", "space": "test", "externalId": "Asset", "version": "v1"},
                    },
                },
            },
        ),
        ("Node", {"instanceType": "node", "space": "test", "externalId": "AssetType"}),
        (
            "DataModel",
            {
                "space": "test",
                "externalId": "assetModel",
                "version": "v1",
                "views": [{"type": "view", "space": "test", "externalId": "Asset", "version": "v1"}],
            },
        ),
    ]
