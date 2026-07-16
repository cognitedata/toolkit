import pytest
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import (
    create_apm_source_data_mappings,
    resolve_source_data_view_ids,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError

CUSTOM_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="CustomOperationView", version="v1")
OTHER_CUSTOM_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="OtherCustomOperationView", version="v1")
CUSTOM_MAINTENANCE_ORDER_VIEW = ViewId(space="sp_customer_idm", external_id="CustomMaintenanceOrderView", version="v1")


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
