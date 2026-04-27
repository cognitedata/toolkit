from collections.abc import Mapping
from typing import Any, Literal

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import UpdatableRequestResource, _get_annotation_origin
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, PrincipalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import KNOWN_TOOLS, AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    InstanceRequest,
    InstanceResponse,
    InstanceSource,
    NodeRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import (
    DatapointSubscriptionRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobRequest
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import HostedExtractorMappingRequest
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    HostedExtractorSourceRequest,
    HostedExtractorSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source._kafka import (
    KafkaSourceRequest,
    KafkaSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.principal import (
    LoginSession,
    Principal,
    ServiceAccountPrincipal,
    UserPrincipal,
)
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import SignalSubscriptionRequest
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine_revision import (
    Disabled,
    LogicalCheckConfig,
    ScheduleConfig,
    SimulatorRoutineConfiguration,
    SimulatorRoutineRevisionRequest,
    SteadyStateDetectionConfig,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import StreamlitResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import WorkflowTriggerRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    WorkflowVersionRequest,
)
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

    @pytest.mark.parametrize(
        "tool_type",
        sorted(t for t in KNOWN_TOOLS if t not in {"callFunction", "queryKnowledgeGraph"}),
    )
    def test_tool_extra_fields_preserved(self, tool_type: str) -> None:
        """Tools must preserve unknown fields so the API can add new properties without breaking deployments."""
        data = {
            "externalId": "agent_1",
            "name": "Agent 1",
            "tools": [
                {
                    "type": tool_type,
                    "name": "my_tool",
                    "description": "A tool for testing",
                    "someNewField": {"key": "value"},
                }
            ],
        }
        agent_request = AgentRequest.model_validate(data)
        assert agent_request.dump() == data


class TestAgentResponse:
    def test_dump_query_tool_unknown_instance_space(self) -> None:
        """The API may return tools with an instanceSpace that the SDK doesn't know about. These should be preserved when dumping."""
        data = {
            "externalId": "agent_1",
            "name": "Agent 1",
            "createdTime": 1731844296876,
            "lastUpdatedTime": 1742795130237,
            "ownerId": "123456789",
            "runtimeVersion": "1.0.0",
            "tools": [
                {
                    "type": "queryKnowledgeGraph",
                    "name": "Query Knowledge Graph",
                    "description": "A tool for querying the knowledge graph",
                    "configuration": {
                        "dataModels": [
                            {"space": "cdf_cdm", "externalId": "CogniteCore", "version": "v1"},
                        ],
                        "instanceSpaces": {"type": "providedAtRuntime", "some_extra": "value"},
                        "version": "v1",
                    },
                }
            ],
        }
        agent_response = AgentResponse.model_validate(data)
        assert agent_response.dump() == data


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

    def test_load_known_acl_with_unknown_scope_and_action(self) -> None:
        data = {
            "name": "Group 1",
            "id": 37,
            "isDeleted": False,
            "capabilities": [
                {"agentsAcl": {"actions": ["READ", "UNKNOWN_ACTION"], "scope": {"anUnknownScope": {"ids": [1, 2, 3]}}}}
            ],
        }
        group = GroupResponse.model_validate(data)

        assert group.dump() == data


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
                logical_check=[
                    LogicalCheckConfig(
                        timeseries_external_id="my_timeseries", aggregate="average", operator="le", value=5.0
                    )
                ],
                steady_state_detection=[
                    SteadyStateDetectionConfig(
                        timeseries_external_id="my_timeseries",
                        aggregate="average",
                        min_section_size=40,
                        var_threshold=4.0,
                        slope_threshold=3.0,
                    )
                ],
            ),
        )

        assert revision.dump() == {
            "externalId": "routine_revision_1",
            "routineExternalId": "routine_1",
            "configuration": {
                "schedule": {"cronExpression": "0 0 * * *", "enabled": True},
                "dataSampling": {"enabled": False},
                "logicalCheck": [
                    {
                        "aggregate": "average",
                        "enabled": True,
                        "operator": "le",
                        "timeseriesExternalId": "my_timeseries",
                        "value": 5.0,
                    }
                ],
                "steadyStateDetection": [
                    {
                        "aggregate": "average",
                        "enabled": True,
                        "minSectionSize": 40,
                        "slopeThreshold": 3.0,
                        "timeseriesExternalId": "my_timeseries",
                        "varThreshold": 4.0,
                    }
                ],
            },
        }


class TestDataSetRequest:
    @pytest.mark.parametrize(
        "data,expected",
        [
            pytest.param(
                {
                    "externalId": "dataset_1",
                    "name": "Dataset 1",
                    "metadata": {"archived": True},  # Non-string value
                },
                {
                    "externalId": "dataset_1",
                    "name": "Dataset 1",
                    # In addition, we need it to be lowercased.
                    "metadata": {"archived": "true"},
                },
                id="read non string metadata",
            ),
            pytest.param(
                {
                    "externalId": "dataset_1",
                    "name": "Dataset 1",
                    "metadata": None,
                },
                {
                    "externalId": "dataset_1",
                    "name": "Dataset 1",
                    "metadata": None,
                },
                id="read metadata None",
            ),
        ],
    )
    def test_read_non_string_metadata(self, data: dict[str, Any], expected: dict[str, Any]) -> None:
        assert DataSetRequest.model_validate(data).dump() == expected


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


class TestNodeRequest:
    def test_node_untyped_behavior(self) -> None:
        node = NodeRequest.model_validate(
            {
                "space": "my_space",
                "externalId": "my_node",
                "type": {
                    "space": "schema_space",
                    "externalId": "my_type",
                },
            }
        )
        assert node.as_id().dump() == {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
        }
        # while dumping the type should not include the instanceType field
        assert node.dump() == {
            "instanceType": "node",
            "space": "my_space",
            "externalId": "my_node",
            "type": {
                "space": "schema_space",
                "externalId": "my_type",
                # No instance type field here.
            },
        }

    def test_convert_node_type_to_untyped(self) -> None:
        my_node_type = NodeId(space="my_space", external_id="my_node")
        my_node_request = NodeRequest(
            space="my_space",
            external_id="instance_node",
            type=my_node_type,
        )
        # Dumped with type
        assert my_node_type.dump() == {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
        }
        # the .type dumped without the instance type field, as it's untyped
        assert my_node_request.dump() == {
            "space": "my_space",
            "externalId": "instance_node",
            "instanceType": "node",
            "type": {
                "space": "my_space",
                "externalId": "my_node",
                # No instance type field here.
            },
        }
        assert my_node_request.type == my_node_type

    def test_serialize_direct_relation_property(self) -> None:
        my_node = NodeRequest(
            space="my_space",
            external_id="my_node",
            sources=[
                InstanceSource(
                    source=ViewId(space="my_schema", external_id="MyView", version="v1"),
                    properties={
                        "myDirectRelation": NodeId(space="my_space", external_id="my_target_node"),
                        "myListableDirectRelation": [
                            NodeId(space="my_space", external_id="my_target_node2"),
                            NodeId(space="my_schema", external_id="my_target_node3"),
                        ],
                    },
                )
            ],
        )
        assert my_node.dump() == {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "sources": [
                {
                    "source": {"space": "my_schema", "externalId": "MyView", "version": "v1", "type": "view"},
                    "properties": {
                        "myDirectRelation": {"space": "my_space", "externalId": "my_target_node"},
                        "myListableDirectRelation": [
                            {"space": "my_space", "externalId": "my_target_node2"},
                            {"space": "my_schema", "externalId": "my_target_node3"},
                        ],
                    },
                }
            ],
        }


class TestStreams:
    def test_stream_request_with_unknown_template(self) -> None:
        data = {
            "externalId": "stream_1",
            "settings": {
                "template": {
                    "name": "unknown_template",
                    "parameters": {"param1": "value1"},
                }
            },
        }
        stream_request = StreamRequest.model_validate(data)
        assert stream_request.dump() == data

    def test_stream_response_with_unknown_template(self) -> None:
        data = {
            "externalId": "stream_1",
            "createdFromTemplate": "unknown_template",
            "type": "Mutable",
            "createdTime": 1731844296876,
        }
        stream_response = StreamResponse.model_validate(data)
        assert stream_response.dump() == data


class TestWorkflowTriggers:
    def test_unknown_trigger_rules(self) -> None:
        data = {
            "externalId": "my_trigger",
            "triggerRule": {"triggerType": "unknown", "some": "value", "that": ["is", "unknown", "to", "toolkit"]},
            "workflowExternalId": "workflow_1",
            "workflowVersion": "v1",
            "authentication": {"nonce": "123"},
        }
        assert WorkflowTriggerRequest._load(data).dump() == data


class TestWorkflowVersion:
    def test_unknown_task_parameters(self) -> None:
        data = {
            "workflowExternalId": "my_workflow",
            "version": "v1",
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "my_task",
                        "type": "unknown_task_type",
                        "parameters": {
                            "param1": "value1",
                            "some": "value",
                            "that": ["is", "unknown", "to", "toolkit"],
                        },
                    }
                ]
            },
        }

        assert WorkflowVersionRequest.model_validate(data).dump() == data


class TestUnknownPrincipalUnion:
    """Principal union routes unknown type strings to UnknownPrincipal."""

    def test_unknown_principal_type(self) -> None:
        data = {
            "id": "principal_unknown_1",
            "type": "NOT_YET_IN_SDK",
            "name": "Future principal",
            "pictureUrl": "https://example.com/p.png",
            "extraFromApi": {"tier": "preview"},
        }
        assert TypeAdapter(Principal).validate_python(data).dump() == data


class TestUnknownTransformationDestinationUnion:
    """Destination union accepts unknown transformation destination kinds."""

    def test_unknown_destination_type(self) -> None:
        data = {
            "externalId": "tf_unknown_dest",
            "name": "Transformation",
            "ignoreNullFields": False,
            "destination": {"type": "futureWarehouse", "catalog": "iceberg", "table": "events"},
        }
        assert TransformationRequest._load(data).dump() == data


class TestUnknownHostedExtractorMappingInputUnion:
    def test_unknown_mapping_input_type(self) -> None:
        data = {
            "externalId": "mapping_1",
            "mapping": {"expression": "true"},
            "published": False,
            "input": {"type": "avro", "schemaRegistryUrl": "https://registry.example.com", "subject": "topic-value"},
        }
        assert HostedExtractorMappingRequest._load(data).dump() == data


class TestUnknownSinkRefUnion:
    def test_unknown_sink_type(self) -> None:
        data = {
            "externalId": "sub_unknown_sink",
            "sink": {"type": "webhook_sink", "url": "https://example.com/hooks/cdf", "secretHeader": "abc"},
            "filter": {"topic": "cognite_workflows"},
        }
        assert SignalSubscriptionRequest._load(data).dump() == data


class TestUnknownSubscriptionFilterUnion:
    def test_unknown_filter_topic(self) -> None:
        data = {
            "externalId": "sub_unknown_filter",
            "sink": {"type": "current_user"},
            "filter": {"topic": "cognite_futureSignals", "routingHint": "priority-high"},
        }
        assert SignalSubscriptionRequest._load(data).dump() == data


class TestUnknownHostedExtractorJobUnions:
    def test_unknown_job_format(self) -> None:
        data = {
            "externalId": "job_unknown_format",
            "destinationId": "dest_1",
            "sourceId": "src_1",
            "format": {"type": "futureBinaryFormat", "endianness": "little", "blockSize": 4096},
        }
        assert HostedExtractorJobRequest._load(data).dump() == data

    def test_unknown_incremental_load_in_rest_config(self) -> None:
        data = {
            "externalId": "job_unknown_incremental",
            "destinationId": "dest_1",
            "sourceId": "src_1",
            "format": {"type": "cognite"},
            "config": {
                "interval": "5m",
                "path": "/ingest",
                "incrementalLoad": {
                    "type": "futureCursor",
                    "opaqueState": {"token": "abc", "seq": 42},
                },
            },
        }
        assert HostedExtractorJobRequest._load(data).dump() == data


class TestUnknownHostedExtractorSourceUnions:
    def test_unknown_source_request_type(self) -> None:
        data = {
            "externalId": "source_future",
            "type": "quantumBridge",
            "endpoint": "https://edge.example.com/v1",
            "handshake": {"protocol": "v2"},
        }
        assert HostedExtractorSourceRequest.validate_python(data).dump() == data

    def test_unknown_source_response_type(self) -> None:
        data = {
            "externalId": "source_future_resp",
            "type": "quantumBridge",
            "createdTime": 1700000000000,
            "lastUpdatedTime": 1700000001000,
            "health": "nominal",
        }
        assert HostedExtractorSourceResponse.validate_python(data).dump() == data


class TestUnknownHostedExtractorAuthenticationUnion:
    def test_unknown_authentication_request_on_kafka_source(self) -> None:
        data = {
            "externalId": "kafka_auth_unknown",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker.example.com", "port": 9092}],
            "authentication": {
                "type": "oauthDeviceCode",
                "deviceUri": "https://idp.example.com/device",
                "userCode": "ABCD",
            },
        }
        assert KafkaSourceRequest._load(data).dump() == data

    def test_unknown_authentication_response_on_kafka_source(self) -> None:
        data = {
            "externalId": "kafka_auth_unknown_resp",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker.example.com", "port": 9092}],
            "createdTime": 1700000000000,
            "lastUpdatedTime": 1700000001000,
            "authentication": {"type": "futureTokenExchange", "expiresIn": 3600},
        }
        assert KafkaSourceResponse._load(data).dump() == data


class TestUnknownDataModelingContainerUnions:
    """Container embeds Constraint, Index, and DataType unions on properties/constraints/indexes."""

    def test_unknown_constraint_index_and_property_data_type(self) -> None:
        data = {
            "space": "dm_space",
            "externalId": "container_future",
            "properties": {
                "propA": {
                    "type": {
                        "type": "hypotheticalGeometry",
                        "srid": 4326,
                        "coordinatesDimension": 3,
                    },
                }
            },
            "constraints": {
                "c_future": {
                    "constraintType": "futureBusinessRule",
                    "expression": "volume > 0",
                    "severity": "warn",
                }
            },
            "indexes": {
                "idx_future": {
                    "indexType": "vectorApproximate",
                    "properties": ["propA"],
                    "dimensions": 128,
                    "metric": "cosine",
                }
            },
        }
        assert ContainerRequest._load(data).dump() == data


class TestUnknownViewPropertyUnion:
    def test_unknown_view_request_property_connection_type(self) -> None:
        data = {
            "space": "view_space",
            "externalId": "view_future",
            "version": "v1",
            "properties": {
                "edge_like": {
                    "connectionType": "future_graph_connector",
                    "opaqueConfig": {"apiVersion": "2026-01"},
                    "name": "External graph",
                }
            },
        }
        assert ViewRequest._load(data).dump() == data


class TestUnknownInstanceUnions:
    def test_unknown_instance_request(self) -> None:
        data = {
            "instanceType": "future_instance_kind",
            "space": "instance_space",
            "externalId": "inst_unknown",
            "opaquePayload": {"routing": "shard-7"},
        }
        assert TypeAdapter(InstanceRequest).validate_python(data).dump() == data

    def test_unknown_instance_response(self) -> None:
        data = {
            "instanceType": "future_instance_kind",
            "space": "instance_space",
            "externalId": "inst_unknown_resp",
            "version": 3,
            "createdTime": 1700000000000,
            "lastUpdatedTime": 1700000001000,
            "opaqueApiFields": {"replicated": True},
        }
        assert TypeAdapter(InstanceResponse).validate_python(data).dump() == data
