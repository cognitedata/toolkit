from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes import Asset
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    MappedProperty,
    NodeId,
    NodeList,
    Text,
    View,
    ViewId,
)

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricToViewMapping, ViewSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMapping,
    AssetCentricMappingList,
    MigrationCSVFileSelector,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.issues import ConversionIssue


class TestAssetCentricMapper:
    def test_map_assets(self, tmp_path: Path) -> None:
        asset_count = 10
        source = AssetCentricMappingList(
            [
                AssetCentricMapping(
                    mapping=MigrationMapping(
                        resourceType="asset",
                        instanceId=NodeId(space="my_space", external_id=f"asset_{i}"),
                        id=1000 + i,
                        ingestionView="cdf_asset_mapping",
                    ),
                    resource=Asset(
                        id=1000 + i,
                        name=f"Asset {i}",
                        # Half of the assets will be missing description and thus have a conversion issue.
                        description=f"Description {i}" if i % 2 == 0 else None,
                    ),
                )
                for i in range(asset_count)
            ]
        )
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text(
            "id,space,externalId,ingestionView\n"
            + "\n".join(f"{1000 + i},my_space,asset_{i},cdf_asset_mapping" for i in range(asset_count))
        )

        selected = MigrationCSVFileSelector(mapping_file, resource_type="asset")

        with monkeypatch_toolkit_client() as client:
            client.migration.view_source.retrieve.return_value = NodeList[ViewSource](
                [
                    ViewSource(
                        external_id="cdf_asset_mapping",
                        resource_type="asset",
                        view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
                        mapping=AssetCentricToViewMapping(
                            to_property_id={
                                "name": "name",
                                "description": "description",
                            }
                        ),
                        last_updated_time=1,
                        created_time=0,
                        version=1,
                    )
                ]
            )
            # Mocking the view to avoid setting all properties we don't use
            cognite_asset = MagicMock(spec=View)
            cognite_asset.properties = {
                "name": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteDescribable"), "name", Text(), True, False, False
                ),
                "description": MappedProperty(
                    ContainerId("cdf_cdm", "CogniteDescribable"), "description", Text(), True, False, False
                ),
            }
            cognite_asset.as_id.return_value = ViewId("cdf_cdm", "CogniteAsset", "v1")
            client.data_modeling.views.retrieve.return_value = [cognite_asset]

            mapper = AssetCentricMapper(client)

            mapper.prepare(selected)

            mapped, issues = mapper.map_chunk(source)

            # We do not assert the exact content of mapped, as that is tested in the
            assert len(mapped) == asset_count
            assert len(issues) == asset_count // 2
            # All issues are the same.
            first_issue = issues[0]
            assert isinstance(first_issue, ConversionIssue)
            assert first_issue.missing_asset_centric_properties == ["description"]

            assert client.migration.view_source.retrieve.call_count == 1
            client.migration.view_source.retrieve.assert_called_with(["cdf_asset_mapping"])
            assert client.data_modeling.views.retrieve.call_count == 1
            client.data_modeling.views.retrieve.assert_called_with([ViewId("cdf_cdm", "CogniteAsset", "v1")])
