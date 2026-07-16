import pytest
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import (
    create_apm_source_data_mappings,
    resolve_apm_source_data_instance_spaces,
    resolve_apm_source_data_view_ids,
    resolve_source_data_view_ids,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError

CUSTOM_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="CustomOperationView", version="v1")
OTHER_CUSTOM_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="OtherCustomOperationView", version="v1")
CUSTOM_MAINTENANCE_ORDER_VIEW = ViewId(space="sp_customer_idm", external_id="CustomMaintenanceOrderView", version="v1")


def _apm_config(
    external_id: str = "APP_CONFIG_V2",
    view_mappings: dict[str, ViewId] | None = None,
    customer_data_space_id: str | None = None,
    customer_data_space_version: str | None = None,
    root_location_configurations: list[RootLocationConfiguration] | None = None,
) -> APMConfigResponse:
    view_mappings_dict: dict[str, JsonValue] | None = None
    if view_mappings:
        view_mappings_dict = {
            entity: {"space": view.space, "externalId": view.external_id, "version": view.version}
            for entity, view in view_mappings.items()
        }
    return APMConfigResponse(
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        customer_data_space_id=customer_data_space_id,
        customer_data_space_version=customer_data_space_version,
        feature_configuration=FeatureConfiguration(
            view_mappings=view_mappings_dict,
            root_location_configurations=root_location_configurations,
        ),
    )


def _location_config(
    external_id: str, data_filter_space_by_key: dict[str, str], view_mapping_by_key: dict[str, ViewId]
) -> InFieldCDMLocationConfigResponse:
    data_filters: dict[str, JsonValue] = {
        key: {"instanceSpaces": [space]} for key, space in data_filter_space_by_key.items()
    }
    view_mappings: dict[str, JsonValue] = {
        key: {"space": view.space, "externalId": view.external_id, "version": view.version}
        for key, view in view_mapping_by_key.items()
    }
    return InFieldCDMLocationConfigResponse(
        instance_type="node",
        space="sp_instance",
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        data_filters=data_filters or None,
        view_mappings=view_mappings or None,
    )


class TestApmSourceDataMapping:
    def test_mapping_validation(self) -> None:
        mappings = create_apm_source_data_mappings(extra="forbid")
        assert mappings, "Mapping creation should not raise an error"


class TestResolveSourceDataViewIds:
    def test_no_locations_returns_empty(self) -> None:
        assert resolve_source_data_view_ids([], "sp_target") == {}

    def test_no_matching_location_returns_empty(self) -> None:
        configs = [_location_config("loc1", {"operations": "sp_other_target"}, {"operation": CUSTOM_OPERATION_VIEW})]

        assert resolve_source_data_view_ids(configs, "sp_target") == {}

    def test_location_without_view_mapping_returns_empty(self) -> None:
        configs = [_location_config("loc1", {"operations": "sp_target"}, {})]

        assert resolve_source_data_view_ids(configs, "sp_target") == {}

    def test_single_location_with_custom_operation_view(self) -> None:
        configs = [_location_config("loc1", {"operations": "sp_target"}, {"operation": CUSTOM_OPERATION_VIEW})]

        assert resolve_source_data_view_ids(configs, "sp_target") == {"operation": CUSTOM_OPERATION_VIEW}

    def test_maintenance_order_resolves_legacy_activity_alias(self) -> None:
        configs = [
            _location_config("loc1", {"maintenanceOrders": "sp_target"}, {"activity": CUSTOM_MAINTENANCE_ORDER_VIEW})
        ]

        assert resolve_source_data_view_ids(configs, "sp_target") == {"maintenanceOrder": CUSTOM_MAINTENANCE_ORDER_VIEW}

    def test_multiple_locations_with_same_view(self) -> None:
        configs = [
            _location_config("loc1", {"operations": "sp_target"}, {"operation": CUSTOM_OPERATION_VIEW}),
            _location_config("loc2", {"operations": "sp_target"}, {"operation": CUSTOM_OPERATION_VIEW}),
        ]

        assert resolve_source_data_view_ids(configs, "sp_target") == {"operation": CUSTOM_OPERATION_VIEW}

    def test_resolves_multiple_types_independently(self) -> None:
        configs = [
            _location_config(
                "loc1",
                {"operations": "sp_target", "notifications": "sp_target"},
                {"operation": CUSTOM_OPERATION_VIEW},
            )
        ]

        assert resolve_source_data_view_ids(configs, "sp_target") == {"operation": CUSTOM_OPERATION_VIEW}

    def test_conflicting_operation_views_raises(self) -> None:
        configs = [
            _location_config("loc1", {"operations": "sp_target"}, {"operation": CUSTOM_OPERATION_VIEW}),
            _location_config("loc2", {"operations": "sp_target"}, {"operation": OTHER_CUSTOM_OPERATION_VIEW}),
        ]

        with pytest.raises(ToolkitMigrationError, match="different operation views"):
            resolve_source_data_view_ids(configs, "sp_target")


class TestResolveApmSourceDataViewIds:
    def test_no_configs_returns_hardcoded_defaults(self) -> None:
        assert resolve_apm_source_data_view_ids([]) == {
            "activity": ViewId(space="APM_SourceData", external_id="APM_Activity", version="1"),
            "operation": ViewId(space="APM_SourceData", external_id="APM_Operation", version="1"),
            "notification": ViewId(space="APM_SourceData", external_id="APM_Notification", version="1"),
        }

    def test_customer_data_space_is_used_when_no_view_mappings(self) -> None:
        configs = [_apm_config(customer_data_space_id="sp_customer", customer_data_space_version="42")]

        resolved = resolve_apm_source_data_view_ids(configs)

        assert resolved["activity"] == ViewId(space="sp_customer", external_id="APM_Activity", version="42")
        assert resolved["operation"] == ViewId(space="sp_customer", external_id="APM_Operation", version="42")

    def test_view_mappings_take_precedence_over_customer_data_space(self) -> None:
        custom_activity_view = ViewId(space="sp_custom", external_id="CustomActivity", version="v3")
        configs = [
            _apm_config(
                view_mappings={"activity": custom_activity_view}, customer_data_space_id="sp_customer_data_space"
            )
        ]

        resolved = resolve_apm_source_data_view_ids(configs)

        assert resolved["activity"] == custom_activity_view
        assert resolved["operation"] == ViewId(space="sp_customer_data_space", external_id="APM_Operation", version="1")

    def test_prefers_app_config_v2_node_over_others(self) -> None:
        configs = [
            _apm_config(external_id="legacy_config", customer_data_space_id="sp_legacy"),
            _apm_config(external_id="APP_CONFIG_V2", customer_data_space_id="sp_v2"),
        ]

        resolved = resolve_apm_source_data_view_ids(configs)

        assert resolved["activity"] == ViewId(space="sp_v2", external_id="APM_Activity", version="1")


class TestResolveApmSourceDataInstanceSpaces:
    def test_no_configs_returns_empty(self) -> None:
        assert resolve_apm_source_data_instance_spaces([]) == set()

    def test_root_location_configurations_are_collected(self) -> None:
        configs = [
            _apm_config(
                root_location_configurations=[
                    RootLocationConfiguration(source_data_instance_space="sp_source_a"),
                    RootLocationConfiguration(source_data_instance_space="sp_source_b"),
                ]
            )
        ]

        assert resolve_apm_source_data_instance_spaces(configs) == {"sp_source_a", "sp_source_b"}

    def test_location_without_source_space_falls_back_to_customer_data_space(self) -> None:
        configs = [
            _apm_config(
                customer_data_space_id="sp_customer",
                root_location_configurations=[RootLocationConfiguration(external_id="loc1")],
            )
        ]

        assert resolve_apm_source_data_instance_spaces(configs) == {"sp_customer"}

    def test_config_without_locations_falls_back_to_customer_data_space(self) -> None:
        configs = [_apm_config(customer_data_space_id="sp_customer")]

        assert resolve_apm_source_data_instance_spaces(configs) == {"sp_customer"}

    def test_config_without_locations_or_customer_data_space_contributes_nothing(self) -> None:
        assert resolve_apm_source_data_instance_spaces([_apm_config()]) == set()
