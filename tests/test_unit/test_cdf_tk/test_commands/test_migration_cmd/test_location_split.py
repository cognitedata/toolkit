from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.commands._migrate.location_split import (
    CDM_SPACE_SUFFIX,
    CFG_SPACE_SUFFIX,
    build_infield_instance_space_name,
    find_shared_legacy_instance_spaces,
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
