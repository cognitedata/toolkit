from unittest.mock import MagicMock

import pytest
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    DataStorage,
    InFieldCDMLocationConfigResponse,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    InFieldConditionMapping,
    InFieldObservationSapStatusMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    create_infield_data_mappings,
    create_infield_schedule_selector,
    resolve_observation_view_id,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError

CUSTOM_OBSERVATION_VIEW = ViewId(space="sp_customer_idm", external_id="ObservationView", version="v1")
OTHER_CUSTOM_OBSERVATION_VIEW = ViewId(space="sp_customer_idm", external_id="OtherObservationView", version="v1")


def _location_config(
    external_id: str, app_instance_space: str | None, observation_view: ViewId | None
) -> InFieldCDMLocationConfigResponse:
    view_mappings: dict[str, JsonValue] | None = None
    if observation_view is not None:
        view_mappings = {
            "observation": [
                {
                    "view": {
                        "space": observation_view.space,
                        "externalId": observation_view.external_id,
                        "version": observation_view.version,
                    }
                }
            ]
        }
    return InFieldCDMLocationConfigResponse(
        instance_type="node",
        space="sp_instance",
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        data_storage=DataStorage(app_instance_space=app_instance_space) if app_instance_space else None,
        view_mappings=view_mappings,
    )


class TestInFieldMapping:
    def test_mapping_validation(self) -> None:
        mappings = create_infield_data_mappings(extra="forbid")
        assert mappings, "Mapping creation should not raise an error"

    def test_schedule_selector_validation(self) -> None:
        selector = create_infield_schedule_selector()
        assert selector, "Schedule selector creation should not raise an error"

    def test_condition_source_view_translates_legacy_version(self) -> None:
        condition_mapping = InFieldConditionMapping(create_infield_data_mappings())
        context = MagicMock()

        result = condition_mapping.convert({"sourceView": "cdf_apm/TemplateItem/v5"}, context)

        assert result.errors == []
        assert result.container_properties == {"sourceView": "cdf_infield/TemplateItem/v1"}

    def test_condition_source_view_unknown_logical_view_errors(self) -> None:
        condition_mapping = InFieldConditionMapping(create_infield_data_mappings())
        context = MagicMock()

        result = condition_mapping.convert({"sourceView": "cdf_apm/Bogus/v1"}, context)

        assert result.container_properties == {}
        assert len(result.errors) == 1
        assert "Unexpected sourceView value" in result.errors[0]


class TestResolveObservationViewId:
    @pytest.mark.parametrize(
        "location_specs, expected",
        [
            pytest.param(
                [("loc1", "sp_other_target", CUSTOM_OBSERVATION_VIEW)],
                None,
                id="no_matching_location",
            ),
            pytest.param(
                [("loc1", "sp_target", None)],
                None,
                id="location_without_observation_config",
            ),
            pytest.param(
                [
                    ("loc1", "sp_target", CUSTOM_OBSERVATION_VIEW),
                    ("loc2", "sp_other_target", OTHER_CUSTOM_OBSERVATION_VIEW),
                ],
                CUSTOM_OBSERVATION_VIEW,
                id="single_location_with_custom_view",
            ),
            pytest.param(
                [
                    ("loc1", "sp_target", CUSTOM_OBSERVATION_VIEW),
                    ("loc2", "sp_target", CUSTOM_OBSERVATION_VIEW),
                ],
                CUSTOM_OBSERVATION_VIEW,
                id="multiple_locations_with_same_view",
            ),
        ],
    )
    def test_resolve_observation_view_id(
        self,
        location_specs: list[tuple[str, str | None, ViewId | None]],
        expected: ViewId | None,
    ) -> None:
        configs = [_location_config(*spec) for spec in location_specs]

        assert resolve_observation_view_id(configs, "sp_target") == expected

    def test_conflicting_observation_views_raises(self) -> None:
        configs = [
            _location_config("loc1", "sp_target", CUSTOM_OBSERVATION_VIEW),
            _location_config("loc2", "sp_target", OTHER_CUSTOM_OBSERVATION_VIEW),
        ]

        with pytest.raises(ToolkitMigrationError, match="different observation views"):
            resolve_observation_view_id(configs, "sp_target")


class TestInFieldObservationSapStatusMapping:
    @pytest.mark.parametrize(
        "status",
        ["Draft", "Completed", "SomeUnrecognizedStatus"],
    )
    def test_business_only_statuses_are_left_to_generic_mapping(self, status: str) -> None:
        mapping = InFieldObservationSapStatusMapping()
        context = MagicMock(destination_properties={"status": MagicMock()})

        result = mapping.convert({"status": status}, context)

        assert result.container_properties == {}
        assert result.errors == []

    @pytest.mark.parametrize(
        "status, expected_container_properties",
        [
            pytest.param(
                "Sent",
                {"status": "Completed", "sapStatus": "Sent", "notificationIdInSap": "sap-notification-123"},
                id="sent",
            ),
            pytest.param(
                "Not sent",
                {"status": "Completed", "sapStatus": "Not sent"},
                id="not_sent_has_no_notification_id",
            ),
            pytest.param(
                "File not sent",
                {
                    "status": "Completed",
                    "sapStatus": "File not sent",
                    "notificationIdInSap": "sap-notification-123",
                },
                id="file_not_sent",
            ),
        ],
    )
    def test_sap_statuses_are_mapped_when_writeback_supported(
        self, status: str, expected_container_properties: dict[str, str]
    ) -> None:
        mapping = InFieldObservationSapStatusMapping()
        context = MagicMock(destination_properties={"sapStatus": MagicMock(), "notificationIdInSap": MagicMock()})

        result = mapping.convert({"status": status, "sourceId": "sap-notification-123"}, context)

        assert result.container_properties == expected_container_properties
        assert result.errors == []

    def test_sent_without_writeback_support_is_left_to_generic_mapping(self) -> None:
        mapping = InFieldObservationSapStatusMapping()
        context = MagicMock(
            destination_properties={"status": MagicMock()},
            mapping=MagicMock(
                destination_view=ViewId(space="cdf_infield", external_id="FieldObservation", version="v1")
            ),
        )

        result = mapping.convert({"status": "Sent", "sourceId": "sap-notification-123"}, context)

        assert result.container_properties == {}
        assert len(result.errors) == 1
        assert (
            "does not have the required target 'sapStatus' and/or 'notificationIdInSap' properties" in result.errors[0]
        )
