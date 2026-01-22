from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import Asset
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DirectRelationReference,
    InstanceApply,
    NodeId,
    NodeList,
    View,
    ViewId,
)

from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.canvas import IndustrialCanvas
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import CreatedSourceSystem, ResourceViewMapping
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequest,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    AssetCentricMapping,
    AssetCentricMappingList,
    MigrationMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper, CanvasMapper, ThreeDAssetMapper
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio.logger import DataLogger, OperationTracker
from tests.data import MIGRATION_DIR


class TestAssetCentricMapper:
    def test_map_assets(
        self, tmp_path: Path, cognite_core_no_3D: DataModel[View], cognite_extractor_views: list[View]
    ) -> None:
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
                    resource=AssetResponse(
                        id=1000 + i,
                        name=f"Asset {i}",
                        source="sap",
                        description=f"Description {i}",
                        createdTime=1,
                        lastUpdatedTime=1,
                        rootId=0,
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

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

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
                            "source": "source",
                        },
                        last_updated_time=1,
                        created_time=0,
                        version=1,
                    )
                ]
            )
            client.migration.created_source_system.retrieve.return_value = NodeList[CreatedSourceSystem](
                [
                    CreatedSourceSystem(
                        space="source_systems",
                        external_id="SAP",
                        source="sap",
                        last_updated_time=1,
                        created_time=0,
                        version=1,
                    ),
                ]
            )
            client.data_modeling.views.retrieve.return_value = cognite_core_no_3D.views + cognite_extractor_views

            mapper = AssetCentricMapper(client)

            mapper.prepare(selected)

            mapped: list[InstanceApply] = []
            for target, item in zip(mapper.map(source), source):
                mapped.append(target)

            # We do not assert the exact content of mapped, as that is tested in the
            # tests for the asset_centric_to_dm function.
            assert len(mapped) == asset_count
            first_asset = mapped[0]
            assert first_asset.sources[0].properties["source"] == DirectRelationReference("source_systems", "SAP")

            assert client.migration.resource_view_mapping.retrieve.call_count == 1
            client.migration.resource_view_mapping.retrieve.assert_called_with(["cdf_asset_mapping"])
            assert client.migration.created_source_system.retrieve.call_count == 1
            assert client.data_modeling.views.retrieve.call_count == 1

    def test_map_chunk_before_prepare_raises_error(self, tmp_path: Path) -> None:
        """Test that calling map_chunk before prepare raises a RuntimeError."""
        source = AssetCentricMapping(
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

        with monkeypatch_toolkit_client() as client:
            mapper = AssetCentricMapper(client)

            # Call map_chunk without calling prepare first
            with pytest.raises(
                RuntimeError,
                match=r"Failed to lookup mapping or view for ingestion view 'cdf_asset_mapping'. Did you forget to call .prepare()?",
            ):
                mapper.map([source])

    def test_prepare_missing_view_source_raises_error(self, tmp_path: Path) -> None:
        """Test that prepare raises ToolkitValueError when view source is not found."""
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text("id,space,externalId,ingestionView\n1001,my_space,asset_1,missing_view_source")

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

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

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

        with monkeypatch_toolkit_client() as client:
            # Return view source but empty view list to simulate missing view in Data Modeling
            client.migration.resource_view_mapping.retrieve.return_value = NodeList[ResourceViewMapping](
                [
                    ResourceViewMapping(
                        external_id="cdf_asset_mapping",
                        resource_type="asset",
                        view_id=ViewId("my_space", "MyAsset", "v1"),
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

            assert "The following ingestion views were not found in Data Modeling" in str(exc_info.value)


class TestThreeDAssetMapper:
    DEFAULTS: ClassVar[dict[str, Any]] = {
        "modelId": 1,
        "revisionId": 1,
    }
    ASSET_ID = "AssetMapping_1"

    @pytest.mark.parametrize(
        "response,lookup_asset,expected",
        [
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=1234,
                    assetInstanceId=NodeReference(space="my_space", externalId="asset_1"),
                    **DEFAULTS,
                ),
                None,
                AssetMappingDMRequest(
                    nodeId=1234,
                    assetInstanceId=NodeReference(space="my_space", externalId="asset_1"),
                    **DEFAULTS,
                ),
                id="Return existing assetInstanceId",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=5678,
                    assetId=37,
                    **DEFAULTS,
                ),
                NodeId(space="my_space", external_id="asset_2"),
                AssetMappingDMRequest(
                    nodeId=5678,
                    assetInstanceId=NodeReference(space="my_space", externalId="asset_2"),
                    **DEFAULTS,
                ),
                id="Lookup and return found assetInstanceId",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=91011,
                    assetId=42,
                    **DEFAULTS,
                ),
                None,
                "Missing asset instance for asset ID 42",
                id="Lookup and return not found issue",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=1213,
                    **DEFAULTS,
                ),
                None,
                "Neither assetInstanceId nor assetId provided for mapping.",
                id="Missing both assetInstanceId and assetId issue",
            ),
        ],
    )
    def test_map_chunk(
        self,
        response: AssetMappingClassicResponse,
        lookup_asset: NodeId | None,
        expected: AssetMappingDMRequest | str,
    ) -> None:
        with monkeypatch_toolkit_client() as client:
            client.migration.lookup.assets.return_value = lookup_asset

            mapper = ThreeDAssetMapper(client)
            logger = MagicMock(spec=DataLogger)
            logger.tracker = MagicMock(spec_set=OperationTracker)
            mapper.logger = logger
            mapped = mapper.map([response])[0]

            if lookup_asset is not None:
                # One for cache population, one for actual call
                assert client.migration.lookup.assets.call_count == 2
                last_call = client.migration.lookup.assets.call_args_list[-1]
                assert last_call.args == (response.asset_id,)

            if isinstance(expected, AssetMappingDMRequest):
                logger.log.assert_not_called()
                logger.tracker.add_issue.assert_not_called()
                assert mapped is not None
                assert mapped.model_dump() == expected.model_dump()
            else:
                _, message = logger.tracker.add_issue.call_args.args
                assert mapped is None, "Expected no mapped result"
                assert message == expected


class TestCanvasMapper:
    def test_map_canvas_with_annotations(self):
        input_canvas_path = MIGRATION_DIR / "canvas" / "annotated_canvas.yaml"
        input_canvas = IndustrialCanvas.load(input_canvas_path.read_text(encoding="utf-8"))
        with monkeypatch_toolkit_client() as client:
            client.migration.lookup.assets.return_value = NodeId(space="my_space", external_id="asset_1")
            client.migration.lookup.events.return_value = NodeId(space="my_space", external_id="event_1")
            client.migration.lookup.files.return_value = NodeId(space="my_space", external_id="file_1")
            client.migration.lookup.time_series.return_value = NodeId(space="my_space", external_id="timeseries_1")
            client.migration.lookup.assets.consumer_view.return_value = ViewId(
                space="cdm_cdm", external_id="CogniteAsset", version="v1"
            )
            client.migration.lookup.events.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteActivity", version="v1"
            )
            client.migration.lookup.files.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteFile", version="v1"
            )
            client.migration.lookup.time_series.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"
            )

            mapper = CanvasMapper(client, dry_run=False, skip_on_missing_ref=False)

            actual = mapper.map([input_canvas])[0]

        assert not actual.container_references
        assert len(actual.fdm_instance_container_references) == len(input_canvas.container_references)

        migrated_dumped_str = actual.dump_yaml()

        unexpected_uuid: list[str] = []
        for item in input_canvas.container_references:
            if item.id_ in migrated_dumped_str:
                unexpected_uuid.append(item.id_)
        # After the migration, there should be no references to the original components of the Canvas.
        assert not unexpected_uuid, f"Found unexpected user data in migrated canvas: {unexpected_uuid}"
