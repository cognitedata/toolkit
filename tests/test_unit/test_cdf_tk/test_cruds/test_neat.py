from pathlib import Path

from openpyxl import Workbook

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._neat import NeatIO
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatSpreadsheet, NeatYAML


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
                "Connection": None,
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

    assert [item.type_.kind for item in resources] == ["Space", "Container", "View", "Node", "DataModel"]


def test_neat_spreadsheet_is_converted_to_data_model_resources(tmp_path: Path) -> None:
    spreadsheet = tmp_path / "asset.xlsx"
    workbook = Workbook()
    metadata = workbook.active
    metadata.title = "Metadata"
    metadata.append(["space", "test"])
    metadata.append(["externalId", "assetModel"])
    metadata.append(["version", "v1"])
    properties = workbook.create_sheet("Properties")
    properties.append(["Definition of Properties"])
    properties.append(
        [
            "View",
            "View Property",
            "Connection",
            "Value Type",
            "Min Count",
            "Max Count",
            "Container",
            "Container Property",
        ]
    )
    properties.append(["Asset", "name", None, "text", 0, 1, "Asset", "name"])
    views = workbook.create_sheet("Views")
    views.append(["Definition of Views"])
    views.append(["View"])
    views.append(["Asset"])
    containers = workbook.create_sheet("Containers")
    containers.append(["Definition of Containers"])
    containers.append(["Container"])
    containers.append(["Asset"])
    enum = workbook.create_sheet("Enum")
    enum.append(["Definition of Enum"])
    enum.append(["Collection", "Value"])
    workbook.save(spreadsheet)

    resource = NeatSpreadsheet(spreadsheet=Path(spreadsheet.name), space="test", external_id="assetModel", version="v1")
    results = list(NeatIO.to_crud_types(resource.model_dump(by_alias=True), tmp_path / "asset.Neat.yaml", resource))
    resources = [item for item in results if isinstance(item, CrudResource)]

    assert (
        [item.type_.kind for item in resources],
        [item.message for item in results if isinstance(item, BuildWarning)],
    ) == (
        ["Space", "Container", "View", "DataModel"],
        ["Spreadsheet table 'Enum': contains no data rows."],
    )
