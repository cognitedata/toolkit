from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import EdgeResponse, NodeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import DataStorage, InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.location_split import (
    CDM_SPACE_SUFFIX,
    CFG_SPACE_SUFFIX,
    build_infield_instance_space_name,
    find_shared_legacy_instance_spaces,
    resolve_app_data_location_split,
)
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

_TEMPLATE_VIEW = ViewId(space="cdf_apm", external_id="Template", version="v8")
_TEMPLATE_ITEM_VIEW = ViewId(space="cdf_apm", external_id="TemplateItem", version="v7")

)
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name


def _apm_config(*roots: RootLocationConfiguration) -> APMConfigResponse:
    return APMConfigResponse(
        space="APM_Config",
        external_id="test_config",
        version=1,
        created_time=0,
        last_updated_time=0,
        feature_configuration=FeatureConfiguration(root_location_configurations=list(roots)),
    )


def _lookup_assets_by_external_id(external_id: str) -> NodeId:
    return NodeId(space="asset_space", external_id=external_id)


def _list_app_data_split_instances(
    filter: InstanceFilter, limit: int | None = None, endpoint: str = "query"
) -> list[NodeResponse | EdgeResponse]:
    source_space = "shared_app"
    source = filter.source
    if source == _TEMPLATE_VIEW:
        return [
            NodeResponse(
                space=source_space,
                external_id="template_a",
                version=1,
                created_time=0,
                last_updated_time=0,
                properties={_TEMPLATE_VIEW: {"rootLocation": {"space": "x", "externalId": "ROOT_A"}}},
            ),
            NodeResponse(
                space=source_space,
                external_id="template_orphan",
                version=1,
                created_time=0,
                last_updated_time=0,
                properties={_TEMPLATE_VIEW: {}},
            ),
        ]
    if source == _TEMPLATE_ITEM_VIEW:
        return [
            NodeResponse(
                space=source_space,
                external_id="item_a",
                version=1,
                created_time=0,
                last_updated_time=0,
                properties={_TEMPLATE_ITEM_VIEW: {}},
            )
        ]
    if filter.instance_type == "edge" and filter.filter is not None:
        filter_dump = str(filter.filter)
        if "referenceTemplateItems" not in filter_dump:
            return []
        return [
            EdgeResponse(
                space=source_space,
                external_id="edge_a",
                version=1,
                created_time=0,
                last_updated_time=0,
                type=NodeId(space="cdf_apm", external_id="referenceTemplateItems"),
                start_node=NodeId(space=source_space, external_id="template_a"),
                end_node=NodeId(space=source_space, external_id="item_a"),
            )
        ]
    return []


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

    def test_skips_roots_without_asset_external_id(self) -> None:
        config = _apm_config(
            RootLocationConfiguration(
                external_id="loc1",
                asset_external_id="ASSET_1",
                app_data_instance_space="shared_app",
                source_data_instance_space="shared_source",
            ),
            RootLocationConfiguration(
                external_id="loc_missing_asset",
                app_data_instance_space="shared_app",
                source_data_instance_space="shared_source",
            ),
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


class TestResolveAppDataCascade:
    def test_resolves_template_item_via_edge_and_orphans_missing_root(self) -> None:
        source_space = "shared_app"
        apm_configs = [
            _apm_config(
                RootLocationConfiguration(
                    external_id="loc1",
                    asset_external_id="ROOT_A",
                    app_data_instance_space=source_space,
                    source_data_instance_space="shared_source",
                ),
                RootLocationConfiguration(
                    external_id="loc2",
                    asset_external_id="ROOT_B",
                    app_data_instance_space=source_space,
                    source_data_instance_space="shared_source",
                ),
            )
        ]
        cdm_configs = [
            InFieldCDMLocationConfigResponse(
                space="cfg",
                external_id="loc1",
                version=1,
                created_time=0,
                last_updated_time=0,
                data_storage=DataStorage(
                    root_location={"space": "asset_space", "externalId": "ROOT_A"},
                    app_instance_space="shared_app_ROOT_A_cdm",
                ),
            ),
            InFieldCDMLocationConfigResponse(
                space="cfg",
                external_id="loc2",
                version=1,
                created_time=0,
                last_updated_time=0,
                data_storage=DataStorage(
                    root_location={"space": "asset_space", "externalId": "ROOT_B"},
                    app_instance_space="shared_app_ROOT_B_cdm",
                ),
            ),
        ]

        with monkeypatch_toolkit_client() as client:
            client.migration.lookup.assets.side_effect = _lookup_assets_by_external_id
            client.tool.instances.list.side_effect = _list_app_data_split_instances
            resolution = resolve_app_data_location_split(
                client,
                source_space=source_space,
                apm_configs=apm_configs,
                cdm_configs=cdm_configs,
            )

        assert resolution.target_space_by_external_id == {
            "template_a": "shared_app_ROOT_A_cdm",
            "item_a": "shared_app_ROOT_A_cdm",
        }
        assert any(orphan.external_id == "template_orphan" for orphan in resolution.orphans)
