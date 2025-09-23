from pathlib import Path
from unittest.mock import MagicMock

import pytest
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

from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMapping,
    AssetCentricMappingList,
    MigrationCSVFileSelector,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.issues import ConversionIssue
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


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
            client.migration.resource_view_mapping.retrieve.return_value = NodeList[ResourceViewMapping](
                [
                    ResourceViewMapping(
                        external_id="cdf_asset_mapping",
                        resource_type="asset",
                        view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
                        property_mapping={
                            "name": "name",
                            "description": "description",
                        },
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
            # tests for the asset_centric_to_dm function.
            assert len(mapped) == asset_count
            assert len(issues) == asset_count // 2
            # All issues are the same.
            first_issue = issues[0]
            assert isinstance(first_issue, ConversionIssue)
            assert first_issue.missing_asset_centric_properties == ["description"]

            assert client.migration.resource_view_mapping.retrieve.call_count == 1
            client.migration.resource_view_mapping.retrieve.assert_called_with(["cdf_asset_mapping"])
            assert client.data_modeling.views.retrieve.call_count == 1
            client.data_modeling.views.retrieve.assert_called_with([ViewId("cdf_cdm", "CogniteAsset", "v1")])

    def test_map_chunk_before_prepare_raises_error(self, tmp_path: Path) -> None:
        """Test that calling map_chunk before prepare raises a RuntimeError."""
        source = AssetCentricMappingList(
            [
                AssetCentricMapping(
                    mapping=MigrationMapping(
                        resourceType="asset",
                        instanceId=NodeId(space="my_space", external_id="asset_1"),
                        id=1001,
                        ingestionView="cdf_asset_mapping",
                    ),
                    resource=Asset(
                        id=1001,
                        name="Asset 1",
                        description="Description 1",
                    ),
                )
            ]
        )

        with monkeypatch_toolkit_client() as client:
            mapper = AssetCentricMapper(client)

            # Call map_chunk without calling prepare first
            with pytest.raises(
                RuntimeError,
                match=r"Failed to lookup mapping or view for ingestion view 'cdf_asset_mapping'. Did you forget to call .prepare()?",
            ):
                mapper.map_chunk(source)

    def test_prepare_missing_view_source_raises_error(self, tmp_path: Path) -> None:
        """Test that prepare raises ToolkitValueError when view source is not found."""
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text("id,space,externalId,ingestionView\n1001,my_space,asset_1,missing_view_source")

        selected = MigrationCSVFileSelector(mapping_file, resource_type="asset")

        with monkeypatch_toolkit_client() as client:
            # Return empty list to simulate missing view source
            client.migration.resource_view_mapping.retrieve.return_value = NodeList[ResourceViewMapping]([])

            mapper = AssetCentricMapper(client)

            with pytest.raises(
                ToolkitValueError, match=r"The following ingestion views were not found: missing_view_source"
            ):
                mapper.prepare(selected)

    def test_prepare_missing_view_in_data_modeling_raises_error(self, tmp_path: Path) -> None:
        """Test that prepare raises ToolkitValueError when view is not found in Data Modeling."""
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text("id,space,externalId,ingestionView\n1001,my_space,asset_1,cdf_asset_mapping")

        selected = MigrationCSVFileSelector(mapping_file, resource_type="asset")

        with monkeypatch_toolkit_client() as client:
            # Return view source but empty view list to simulate missing view in Data Modeling
            client.migration.resource_view_mapping.retrieve.return_value = NodeList[ResourceViewMapping](
                [
                    ResourceViewMapping(
                        external_id="cdf_asset_mapping",
                        resource_type="asset",
                        view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
                        property_mapping={
                            "name": "name",
                            "description": "description",
                        },
                        last_updated_time=1,
                        created_time=0,
                        version=1,
                    )
                ]
            )
            # Return empty list to simulate missing view in Data Modeling
            client.data_modeling.views.retrieve.return_value = []

            mapper = AssetCentricMapper(client)

            with pytest.raises(ToolkitValueError) as exc_info:
                mapper.prepare(selected)

            assert (
                str(exc_info.value)
                == "The following ingestion views were not found in Data Modeling: ViewId(space='cdf_cdm', external_id='CogniteAsset', version='v1')"
            )
