from collections.abc import Callable

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetAggregateItem, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.conversion import TargetSpaceResolutionError
from cognite_toolkit._cdf_tk.commands._migrate.location_split import (
    CDM_SPACE_SUFFIX,
    CFG_SPACE_SUFFIX,
    AssetExternalIdTargetSpaceResolver,
    build_infield_instance_space_name,
    find_shared_legacy_instance_spaces,
)
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

_NOTIFICATION_VIEW = ViewId(space="idm", external_id="CogniteNotification", version="v1")


def _notification_node(external_id: str, asset_external_id: str) -> NodeResponse:
    return NodeResponse(
        space="source_space",
        external_id=external_id,
        version=1,
        last_updated_time=0,
        created_time=0,
        properties={_NOTIFICATION_VIEW: {"assetExternalId": asset_external_id}},
    )


def _asset_response(internal_id: int, external_id: str, path_ids: list[int] | None = None) -> AssetResponse:
    return AssetResponse(
        id=internal_id,
        external_id=external_id,
        name=external_id,
        root_id=path_ids[0] if path_ids else internal_id,
        created_time=0,
        last_updated_time=0,
        aggregates=AssetAggregateItem(path=[{"id": segment_id} for segment_id in path_ids])
        if path_ids is not None
        else None,
    )


def _apm_config(*roots: RootLocationConfiguration) -> APMConfigResponse:
    return APMConfigResponse(
        space="APM_Config",
        external_id="test_config",
        version=1,
        created_time=0,
        last_updated_time=0,
        feature_configuration=FeatureConfiguration(root_location_configurations=list(roots)),
    )


class TestFindSharedLegacyInstanceSpaces:
    def test_returns_empty_when_each_location_has_distinct_spaces(self) -> None:
        config = _apm_config(
            RootLocationConfiguration(
                external_id="loc1",
                asset_external_id="ASSET_1",
                app_data_instance_space="app_a",
                source_data_instance_space="source_a",
            ),
            RootLocationConfiguration(
                external_id="loc2",
                asset_external_id="ASSET_2",
                app_data_instance_space="app_b",
                source_data_instance_space="source_b",
            ),
        )
        assert find_shared_legacy_instance_spaces([config]) == set()

    def test_detects_shared_app_and_source_spaces(self) -> None:
        config = _apm_config(
            RootLocationConfiguration(
                external_id="loc1",
                asset_external_id="ASSET_1",
                app_data_instance_space="shared_app",
                source_data_instance_space="shared_source",
            ),
            RootLocationConfiguration(
                external_id="loc2",
                asset_external_id="ASSET_2",
                app_data_instance_space="shared_app",
                source_data_instance_space="shared_source",
            ),
        )
        assert find_shared_legacy_instance_spaces([config]) == {"shared_app", "shared_source"}

    def test_same_space_as_app_and_source_for_one_location_is_not_shared(self) -> None:
        config = _apm_config(
            RootLocationConfiguration(
                external_id="loc1",
                asset_external_id="ASSET_1",
                app_data_instance_space="same_space",
                source_data_instance_space="same_space",
            )
        )
        assert find_shared_legacy_instance_spaces([config]) == set()


class TestBuildInfieldInstanceSpaceName:
    def test_unshared_keeps_legacy_suffix(self) -> None:
        assert (
            build_infield_instance_space_name("sp_infield_oid_app_data", CDM_SPACE_SUFFIX)
            == "sp_infield_oid_app_data_cdm"
        )
        assert (
            build_infield_instance_space_name("sp_infield_oid_app_data", CFG_SPACE_SUFFIX)
            == "sp_infield_oid_app_data_cfg"
        )

    def test_shared_inserts_sanitized_asset_external_id(self) -> None:
        assert (
            build_infield_instance_space_name(
                "shared_app",
                CDM_SPACE_SUFFIX,
                asset_external_id="WMT:VAL",
                shared=True,
            )
            == "shared_app_WMTVAL_cdm"
        )

    def test_shared_truncates_to_dms_space_limit(self) -> None:
        legacy = "sp_very_long_legacy_space_name"
        asset = "VERY_LONG_ROOT_ASSET_EXTERNAL_ID"
        result = build_infield_instance_space_name(
            legacy,
            CDM_SPACE_SUFFIX,
            asset_external_id=asset,
            shared=True,
        )
        expected_candidate = f"{legacy}_{asset}{CDM_SPACE_SUFFIX}"
        assert len(expected_candidate) > 43
        assert result == fix_invalid_space_name(expected_candidate)
        assert len(result) <= 43


class TestAssetExternalIdTargetSpaceResolver:
    @staticmethod
    def _assets_retrieve_side_effect(
        location_assets: list[AssetResponse], assets_with_path: list[AssetResponse]
    ) -> Callable[..., list[AssetResponse]]:
        def _side_effect(*args: object, **kwargs: object) -> list[AssetResponse]:
            return assets_with_path if kwargs.get("aggregated_properties") else location_assets

        return _side_effect

    def test_resolves_location_asset_that_is_not_hierarchy_root(self) -> None:
        # PLANT (id=1) -> PLANT-UNIT-A (id=2, the configured location asset) -> the notification's asset (id=3).
        target_by_root_asset = {"PLANT-UNIT-A": "unit_a_target"}
        location_assets = [_asset_response(2, "PLANT-UNIT-A")]
        notification_asset = _asset_response(3, "PLANT-UNIT-A-PUMP-017", path_ids=[1, 2, 3])

        with monkeypatch_toolkit_client() as client:
            client.assets.retrieve.side_effect = self._assets_retrieve_side_effect(
                location_assets, [notification_asset]
            )
            resolver = AssetExternalIdTargetSpaceResolver(
                client, _NOTIFICATION_VIEW, "assetExternalId", target_by_root_asset
            )
            node = _notification_node("notification1", "PLANT-UNIT-A-PUMP-017")
            resolver.prepare_page([node])
            target_space = resolver.resolve(node)

        assert target_space == "unit_a_target"

    def test_closest_location_asset_in_path_wins(self) -> None:
        # Both PLANT (id=1) and PLANT-UNIT-A (id=2) are configured locations; PLANT-UNIT-A is the closer ancestor.
        target_by_root_asset = {"PLANT": "plant_target", "PLANT-UNIT-A": "unit_a_target"}
        location_assets = [_asset_response(1, "PLANT"), _asset_response(2, "PLANT-UNIT-A")]
        notification_asset = _asset_response(3, "PLANT-UNIT-A-PUMP-017", path_ids=[1, 2, 3])

        with monkeypatch_toolkit_client() as client:
            client.assets.retrieve.side_effect = self._assets_retrieve_side_effect(
                location_assets, [notification_asset]
            )
            resolver = AssetExternalIdTargetSpaceResolver(
                client, _NOTIFICATION_VIEW, "assetExternalId", target_by_root_asset
            )
            node = _notification_node("notification1", "PLANT-UNIT-A-PUMP-017")
            resolver.prepare_page([node])
            target_space = resolver.resolve(node)

        assert target_space == "unit_a_target"

    def test_raises_when_no_location_asset_is_in_path(self) -> None:
        target_by_root_asset = {"PLANT-UNIT-A": "unit_a_target"}
        location_assets = [_asset_response(2, "PLANT-UNIT-A")]
        # The notification's asset belongs to an unrelated hierarchy (id=9), not under PLANT-UNIT-A.
        notification_asset = _asset_response(3, "OTHER-ASSET", path_ids=[9, 3])

        with monkeypatch_toolkit_client() as client:
            client.assets.retrieve.side_effect = self._assets_retrieve_side_effect(
                location_assets, [notification_asset]
            )
            resolver = AssetExternalIdTargetSpaceResolver(
                client, _NOTIFICATION_VIEW, "assetExternalId", target_by_root_asset
            )
            node = _notification_node("notification1", "OTHER-ASSET")
            resolver.prepare_page([node])
            with pytest.raises(TargetSpaceResolutionError, match="not located under any location asset"):
                resolver.resolve(node)
