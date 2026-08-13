import pytest

from cognite_toolkit._cdf_tk.client.identifiers import EdgeId, EdgeTypeId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    EdgeOtherSide,
    InstanceMappingError,
    LocationSplitInstanceIdMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.location_split import (
    CDM_SPACE_SUFFIX,
    CFG_SPACE_SUFFIX,
    build_infield_instance_space_name,
    find_shared_legacy_instance_spaces,
    resolve_target_space_via_parent_edge,
    resolve_target_space_via_parent_property,
    resolve_target_space_via_root_location,
)
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

_TEMPLATE_VIEW = ViewId(space="cdf_apm", external_id="Template", version="v8")
_TEMPLATE_ITEM_VIEW = ViewId(space="cdf_apm", external_id="TemplateItem", version="v7")
_CONDITIONAL_ACTION_VIEW = ViewId(space="cdf_apm", external_id="ConditionalAction", version="v1")
_PARENT_EDGE = EdgeTypeId(type=NodeId(space="cdf_apm", external_id="referenceTemplateItems"), direction="inwards")


def _apm_config(*roots: RootLocationConfiguration) -> APMConfigResponse:
    return APMConfigResponse(
        space="APM_Config",
        external_id="test_config",
        version=1,
        created_time=0,
        last_updated_time=0,
        feature_configuration=FeatureConfiguration(root_location_configurations=list(roots)),
    )


def _node(view_id: ViewId, external_id: str, properties: dict, space: str = "shared_app") -> NodeResponse:
    return NodeResponse(
        space=space,
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        properties={view_id: properties},
    )


def _instance_id_mapper() -> LocationSplitInstanceIdMapper:
    with monkeypatch_toolkit_client() as client:
        client.migration.instance_space_relocation_source.retrieve.return_value = []
        return LocationSplitInstanceIdMapper(client, "shared_app")


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


class TestResolveTargetSpaceViaRootLocation:
    def test_resolves_matching_root_location(self) -> None:
        node = _node(_TEMPLATE_VIEW, "template_a", {"rootLocation": {"space": "x", "externalId": "ROOT_A"}})

        target_space = resolve_target_space_via_root_location(node, _TEMPLATE_VIEW, {"ROOT_A": "shared_app_ROOT_A"})

        assert target_space == "shared_app_ROOT_A"

    def test_missing_root_location_raises(self) -> None:
        node = _node(_TEMPLATE_VIEW, "template_orphan", {})

        with pytest.raises(InstanceMappingError, match="missing rootLocation"):
            resolve_target_space_via_root_location(node, _TEMPLATE_VIEW, {"ROOT_A": "shared_app_ROOT_A"})


class TestResolveTargetSpaceViaParentEdge:
    def test_resolves_via_registered_parent(self) -> None:
        node = _node(_TEMPLATE_ITEM_VIEW, "item_a", {})
        instance_id_mapper = _instance_id_mapper()
        instance_id_mapper.register("template_a", "shared_app_ROOT_A")
        other_side_by_edge_type = {
            _PARENT_EDGE: [
                EdgeOtherSide(
                    edge_id=EdgeId(space="shared_app", external_id="edge_a"),
                    other_side=NodeId(space="shared_app", external_id="template_a"),
                )
            ]
        }

        target_space = resolve_target_space_via_parent_edge(
            node, _PARENT_EDGE, other_side_by_edge_type, instance_id_mapper
        )

        assert target_space == "shared_app_ROOT_A"

    def test_no_parent_edge_raises(self) -> None:
        node = _node(_TEMPLATE_ITEM_VIEW, "item_orphan", {})
        instance_id_mapper = _instance_id_mapper()

        with pytest.raises(InstanceMappingError, match="no inbound"):
            resolve_target_space_via_parent_edge(node, _PARENT_EDGE, {}, instance_id_mapper)


class TestResolveTargetSpaceViaParentProperty:
    def test_resolves_via_registered_parent(self) -> None:
        node = _node(
            _CONDITIONAL_ACTION_VIEW, "action_a", {"parentObject": {"space": "shared_app", "externalId": "item_a"}}
        )
        instance_id_mapper = _instance_id_mapper()
        instance_id_mapper.register("item_a", "shared_app_ROOT_A")

        target_space = resolve_target_space_via_parent_property(
            node, _CONDITIONAL_ACTION_VIEW, "parentObject", instance_id_mapper
        )

        assert target_space == "shared_app_ROOT_A"
