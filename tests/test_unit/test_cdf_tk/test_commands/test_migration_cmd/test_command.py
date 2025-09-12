from pathlib import Path

import pytest
import responses
from cognite.client.data_classes import Asset, AssetList
from cognite.client.data_classes.data_modeling import ContainerId, DirectRelation, MappedProperty, Text, View

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import _ASSET_ID, create_default_mappings
from cognite_toolkit._cdf_tk.storageio import AssetIO, InstanceIO


class TestMigrationCommand:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_migrate(self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, tmp_path: Path) -> None:
        asset_mapping = next(mapping for mapping in create_default_mappings() if mapping.external_id == _ASSET_ID)
        config = toolkit_config
        assets = AssetList(
            [
                Asset(
                    id=1000 + i,
                    external_id=f"asset_{i}",
                    name=f"Asset {i}",
                    description=f"This is Asset {i}",
                    last_updated_time=1,
                    created_time=0,
                    parent_external_id="asset_0" if i > 0 else None,
                )
                for i in range(2)
            ]
        )
        space = "my_space"
        csv_content = "id,space,externalId,ingestionView\n" + "\n".join(
            f"{1000 + i},{space},asset_{i},{asset_mapping.external_id}" for i in range(len(assets))
        )

        # Lookup of the mapping in the Migration Model
        mapping_node_response = asset_mapping.dump(context="api")
        mapping_node_response.update({"createdTime": 0, "lastUpdatedTime": 0, "version": 1})
        sources = mapping_node_response.pop("sources", [])
        if sources:
            mapping_view_id = asset_mapping.sources[0].source
            mapping_node_response["properties"] = {
                mapping_view_id.space: {
                    f"{mapping_view_id.external_id}/{mapping_view_id.version}": sources[0]["properties"]
                }
            }
        rsps.post(
            config.create_api_url("models/instances/byids"),
            json={"items": [mapping_node_response]},
            status=200,
        )

        # Lookup CogniteAsset, this is not the full model, just the properties we need for the
        # migration
        default_prop_args = dict(nullable=True, immutable=False, auto_increment=False)
        default_view_args = dict(
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            implements=[],
            writable=True,
            used_for="node",
            is_global=True,
            filter=None,
        )
        cognite_asset = View(
            space="cdf_cdm",
            external_id="CogniteAsset",
            version="v1",
            properties={
                "name": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteDescribable"), "name", Text(), **default_prop_args
                ),
                "description": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteDescribable"), "description", Text(), **default_prop_args
                ),
                "source": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteSourceable"), "name", DirectRelation(), **default_prop_args
                ),
                "tags": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteDescribable"), "tags", Text(is_list=True), **default_prop_args
                ),
            },
            **default_view_args,
        )
        rsps.post(
            config.create_api_url("models/views/byids"),
            json={"items": [cognite_asset.dump()]},
        )

        # Asset retrieve ids
        rsps.post(
            config.create_api_url("/assets/byids"),
            json={"items": [asset.dump() for asset in assets]},
            status=200,
        )
        rsps.post(
            config.create_api_url("/models/instances"),
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "space": space,
                        "externalId": f"asset_{i}",
                        "version": 1,
                        "wasModified": True,
                        "createdTime": 1,
                        "lastUpdatedTime": 1,
                    }
                    for i in range(len(assets))
                ]
            },
            status=200,
        )

        csv_file = tmp_path / "migration.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)

        command.migrate(
            selected=MigrationCSVFileSelector(csv_file, resource_type="asset"),
            data=AssetCentricMigrationIOAdapter(client, AssetIO(client), InstanceIO(client)),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
