from collections.abc import Mapping
from typing import Any, Literal

import pytest

from cognite_toolkit._cdf_tk.client._resource_base import UpdatableRequestResource, _get_annotation_origin
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import PrincipalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import (
    DatapointSubscriptionRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupResponse
from cognite_toolkit._cdf_tk.client.resource_classes.principal import (
    LoginSession,
    ServiceAccountPrincipal,
    UserPrincipal,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine_revision import (
    Disabled,
    ScheduleConfig,
    SimulatorRoutineConfiguration,
    SimulatorRoutineRevisionRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import StreamlitResponse
from cognite_toolkit._cdf_tk.feature_flags import Flags
from tests.test_unit.test_cdf_tk.test_client.data import (
    CDFResource,
    get_example_minimum_responses,
    iterate_cdf_resources,
)


class TestAPIDataClasses:
    @pytest.mark.parametrize("resource", list(iterate_cdf_resources()))
    def test_serialization(self, resource: CDFResource) -> None:
        response_cls = resource.response_cls
        request_cls = resource.request_cls
        data = resource.example_data

        response_instance = response_cls.model_validate(data)
        if resource.is_as_request_possible:
            request_instance = response_instance.as_request_resource()
            assert isinstance(request_instance, request_cls)
            resource_id = request_instance.as_id()
            try:
                hash(resource_id)
            except TypeError:
                assert False, f"Resource ID {resource_id} is not hashable"
            assert isinstance(str(resource_id), str), "Resource ID string representation failed"
        if resource.is_dump_equal_to_example:
            assert response_instance.dump() == data

    @pytest.mark.parametrize("resource", list(iterate_cdf_resources()))
    def test_as_update(self, resource: CDFResource) -> None:
        if not resource.is_as_request_possible:
            return
        request_instance = resource.request_instance
        if not isinstance(request_instance, UpdatableRequestResource):
            return
        update_data = request_instance.as_update(mode="patch")
        assert isinstance(update_data, dict)
        assert "update" in update_data

    @pytest.mark.skipif(
        not Flags.v08.is_enabled(), reason="Extra fields are only supported when the v0.8 flag is enabled"
    )
    def test_dump_exclude_extra(self) -> None:
        """Tests that extra fields can be excluded when dumping a data class.
        Using AssetRequest as an example.
        """
        raw = {
            "externalId": "asset_1",
            "name": "Asset 1",
            "description": "An example asset",
            "metadata": {"key": "value"},
            "extra_field": "extra_value",
        }
        asset_request = AssetRequest.model_validate(raw)
        dumped_with_extra = asset_request.dump()
        assert dumped_with_extra == raw

        dumped_without_extra = asset_request.dump(exclude_extra=True)
        assert dumped_without_extra == {
            "externalId": "asset_1",
            "name": "Asset 1",
            "description": "An example asset",
            "metadata": {"key": "value"},
        }


class TestRequestUpdateable:
    """We use the AssetRequest class as a representative example of RequestUpdateable."""

    @pytest.mark.parametrize(
        "request_instance, mode, expected_update",
        [
            pytest.param(
                AssetRequest(externalId="asset_1", name="Asset 1"),
                "patch",
                {"externalId": "asset_1", "update": {"name": {"set": "Asset 1"}}},
                id="Patch with only required field",
            ),
            pytest.param(
                AssetRequest(externalId="asset_1", name="Asset 1"),
                "replace",
                {
                    "externalId": "asset_1",
                    "update": {
                        "dataSetId": {"setNull": True},
                        "description": {"setNull": True},
                        "geoLocation": {"setNull": True},
                        "labels": {"set": []},
                        "metadata": {"set": {}},
                        "name": {"set": "Asset 1"},
                        "source": {"setNull": True},
                    },
                },
                id="Replace with only required field",
            ),
            pytest.param(
                AssetRequest(
                    externalId="asset_1", name="Asset 1", metadata={"key": "value"}, labels=[{"externalId": "label_1"}]
                ),
                "patch",
                {
                    "externalId": "asset_1",
                    "update": {
                        "name": {"set": "Asset 1"},
                        "metadata": {"add": {"key": "value"}},
                        "labels": {"add": [{"externalId": "label_1"}]},
                    },
                },
                id="Patch with container fields",
            ),
            pytest.param(
                AssetRequest(
                    externalId="asset_1", name="Asset 1", metadata={"key": "value"}, labels=[{"externalId": "label_1"}]
                ),
                "replace",
                {
                    "externalId": "asset_1",
                    "update": {
                        "dataSetId": {"setNull": True},
                        "description": {"setNull": True},
                        "geoLocation": {"setNull": True},
                        "labels": {"set": [{"externalId": "label_1"}]},
                        "metadata": {"set": {"key": "value"}},
                        "name": {"set": "Asset 1"},
                        "source": {"setNull": True},
                    },
                },
                id="Replace with container fields",
            ),
        ],
    )
    def test_as_update(
        self,
        request_instance: UpdatableRequestResource,
        mode: Literal["patch", "replace"],
        expected_update: dict[str, Any],
    ) -> None:
        assert request_instance.as_update(mode=mode) == expected_update


class TestAgentRequest:
    def test_allow_unknown_tool(self) -> None:
        data = {
            "externalId": "agent_1",
            "name": "Agent 1",
            "tools": [
                {
                    "type": "unknown_tool",
                    "name": "Custom Tool",
                    "description": "A tool that is not yet recognized",
                }
            ],
        }
        agent_request = AgentRequest.model_validate(data)
        assert agent_request.dump() == data


class TestStreamlit:
    FILE_RESPONSE_DATA: Mapping[str, Any] = {
        "id": 487285814928547,
        "externalId": "myapp",
        "name": "MySuperApp-source.json",
        "directory": "/streamlit-apps/",
        "metadata": {
            "cdf-toolkit-app-hash": "c59dc2b6",
            "creator": "doctrino@github.com",
            "description": "This is a super app",
            "entrypoint": "main.py",
            "name": "MySuperApp",
            "published": "true",
            "theme": "Light",
            "thumbnail": "data:image/webp;base64,....",
        },
        "dataSetId": 5816056366346276,
        "uploaded": True,
        "uploadedTime": 1742795130067,
        "createdTime": 1731844296876,
        "lastUpdatedTime": 1742795130237,
    }

    def test_api_serialization(self) -> None:
        streamlit_response = StreamlitResponse.model_validate(self.FILE_RESPONSE_DATA)
        filemetadata_response = FileMetadataResponse.model_validate(self.FILE_RESPONSE_DATA)

        streamlit_request = streamlit_response.as_request_resource()
        filemetadata_request = filemetadata_response.as_request_resource()

        assert streamlit_request.dump() == filemetadata_request.dump(), (
            "StreamlitRequest and FileMetadataRequest dumps should be the same"
        )

    def test_as_update(self) -> None:
        request = StreamlitResponse.model_validate(self.FILE_RESPONSE_DATA).as_request_resource()

        assert request.as_update(mode="replace") == {
            "externalId": "myapp",
            "update": {
                "dataSetId": {"set": 5816056366346276},
                "directory": {"set": "/streamlit-apps/"},
                "metadata": {
                    "set": {
                        "cdf-toolkit-app-hash": "c59dc2b6",
                        "creator": "doctrino@github.com",
                        "description": "This is a super app",
                        "entrypoint": "main.py",
                        "name": "MySuperApp",
                        "published": "true",
                        "theme": "Light",
                        "thumbnail": "data:image/webp;base64,....",
                    }
                },
            },
        }


class TestGroupResponse:
    def test_load_table_scope(self) -> None:
        data = {
            "name": "Group 1",
            "capabilities": [
                {
                    "rawAcl": {
                        "actions": ["READ"],
                        "scope": {"tableScope": {"dbsToTables": {"contextualizationState": {}, "ingestion": {}}}},
                    }
                }
            ],
            "id": 37,
            "isDeleted": False,
        }

        GroupResponse.model_validate(data)


class TestPrincipalSerialization:
    def test_service_account_principal_round_trip(self) -> None:
        data = get_example_minimum_responses(ServiceAccountPrincipal)
        principal = ServiceAccountPrincipal.model_validate(data)
        assert principal.dump() == data

    def test_user_principal_round_trip(self) -> None:
        data = get_example_minimum_responses(UserPrincipal)
        principal = UserPrincipal.model_validate(data)
        assert principal.dump() == data

    def test_service_account_principal_as_id(self) -> None:
        data = get_example_minimum_responses(ServiceAccountPrincipal)
        principal = ServiceAccountPrincipal.model_validate(data)
        principal_id = principal.as_id()
        assert isinstance(principal_id, PrincipalId)
        assert principal_id.id == data["id"]

    def test_user_principal_as_id(self) -> None:
        data = get_example_minimum_responses(UserPrincipal)
        principal = UserPrincipal.model_validate(data)
        principal_id = principal.as_id()
        assert isinstance(principal_id, PrincipalId)
        assert principal_id.id == data["id"]

    def test_login_session_round_trip(self) -> None:
        data = get_example_minimum_responses(LoginSession)
        session = LoginSession.model_validate(data)
        dumped = session.dump()
        assert dumped["id"] == data["id"]
        assert dumped["createdTime"] == data["createdTime"]
        assert dumped["status"] == data["status"]

    def test_login_session_with_deactivated_time(self) -> None:
        data = {**get_example_minimum_responses(LoginSession), "status": "REVOKED", "deactivatedTime": 1622547900000}
        session = LoginSession.model_validate(data)
        assert session.status == "REVOKED"
        assert session.deactivated_time == 1622547900000


class TestDatapointSubscriptionUpdateRequest:
    def test_as_update(self) -> None:
        request = DatapointSubscriptionRequest(
            external_id="subscription_1",
            name="Subscription 1",
            partition_count=1,
        )

        assert request.as_update().dump() == {
            "externalId": "subscription_1",
            "update": {
                "name": {"set": "Subscription 1"},
                "description": {"setNull": True},
                "dataSetId": {"setNull": True},
            },
        }


class TestSimulatorRoutineRevision:
    def test_unset_required_fields_are_dumped(self) -> None:
        revision = SimulatorRoutineRevisionRequest(
            external_id="routine_revision_1",
            routine_external_id="routine_1",
            configuration=SimulatorRoutineConfiguration(
                schedule=ScheduleConfig(cron_expression="0 0 * * *"),
                data_sampling=Disabled(),
                logical_check=[],
                steady_state_detection=[],
            ),
        )

        assert revision.dump() == {
            "externalId": "routine_revision_1",
            "routineExternalId": "routine_1",
            "configuration": {
                "schedule": {"cronExpression": "0 0 * * *", "enabled": True},
                "dataSampling": {"enabled": False},
                "logicalCheck": [],
                "steadyStateDetection": [],
            },
        }


class TestDataSetRequest:
    def test_read_non_string_metadata(self) -> None:
        data = {
            "externalId": "dataset_1",
            "name": "Dataset 1",
            "metadata": {"archived": True},  # Non-string value
        }
        dataset_request = DataSetRequest.model_validate(data)
        assert dataset_request.dump() == {
            "externalId": "dataset_1",
            "name": "Dataset 1",
            "metadata": {"archived": "True"},
        }


class TestGetAnnotationOrigin:
    @pytest.mark.parametrize(
        "annotation, expected_origin",
        [
            (list[int], list),
            (dict[str, int], dict),
            (int | None, int),
            (str | None, str),
            (Metadata, dict),
            (Metadata | None, dict),
        ],
    )
    def test_get_annotation_origin_of_annotated(self, annotation: Any, expected_origin: Any) -> None:
        assert _get_annotation_origin(annotation) == expected_origin
