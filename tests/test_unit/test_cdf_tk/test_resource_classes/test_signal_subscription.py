from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SignalSinkId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SignalSubscriptionRequest,
    SignalSubscriptionResponse,
    UnknownSubscriptionFilter,
)
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._resource_ios.hosted_extractors import (
    HostedExtractorDestinationIO,
    HostedExtractorSourceIO,
)
from cognite_toolkit._cdf_tk.cruds._resource_ios.signal_sink import SignalSinkIO
from cognite_toolkit._cdf_tk.cruds._resource_ios.signal_subscription import SignalSubscriptionIO
from cognite_toolkit._cdf_tk.cruds._resource_ios.workflow import WorkflowIO
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.signal_subscription import SignalSubscriptionYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources

_INTEGRATIONS_SUB = {
    "externalId": "sub-int",
    "sink": {"type": "email", "externalId": "sink-1"},
    "filter": {
        "topic": "cognite_integrations",
        "resource": "extractor-abc",
        "severity": "error",
        "extractorExternalId": "ext-1",
        "extractorVersion": "1.2.0",
    },
}

_WORKFLOWS_SUB = {
    "externalId": "sub-wf",
    "sink": {"type": "user", "externalId": "sink-2"},
    "filter": {"topic": "cognite_workflows", "severity": "warning"},
}

_HOSTED_EXTRACTORS_SUB = {
    "externalId": "sub-he",
    "sink": {"type": "current_user"},
    "filter": {
        "topic": "cognite_hosted_extractors",
        "severity": "info",
        "sourceExternalId": "my-source",
        "destinationExternalId": "my-dest",
    },
}


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "sub-1", "sink": {"type": "email", "externalId": "s1"}},
        {"Missing required field: 'filter'"},
        id="missing-filter",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "filter": {"topic": "cognite_workflows"},
        },
        {"Missing required field: 'sink'"},
        id="missing-sink",
    )
    yield pytest.param(
        {
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "cognite_workflows"},
        },
        {"Missing required field: 'externalId'"},
        id="missing-external-id",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "invalid_topic"},
        },
        {
            "In field filter input tag 'invalid_topic' found using 'topic' "
            "does not match any of the expected tags: 'cognite_integrations', 'cognite_workflows', "
            "'cognite_hosted_extractors'",
        },
        id="invalid-topic",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "cognite_integrations"},
            "unknownField": "x",
        },
        {"Unknown field: 'unknownField'"},
        id="unknown-field",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "sink": {"type": "email"},
            "filter": {"topic": "cognite_workflows"},
        },
        {"In sink.email missing required field: 'externalId'"},
        id="email-sink-missing-external-id",
    )


class TestSignalSubscriptionYAML:
    @pytest.mark.parametrize("data", list(find_resources("Subscription", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid_subscription(self, data: dict[str, object]) -> None:
        loaded = SignalSubscriptionYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_subscription_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SignalSubscriptionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors

    def test_as_id(self) -> None:
        loaded = SignalSubscriptionYAML.model_validate(_WORKFLOWS_SUB)
        assert loaded.as_id() == ExternalId(external_id="sub-wf")

    def test_current_user_sink_no_external_id(self) -> None:
        loaded = SignalSubscriptionYAML.model_validate(_HOSTED_EXTRACTORS_SUB)
        assert loaded.sink.type == "current_user"
        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert "externalId" not in dumped["sink"]


class TestSignalSubscriptionResourceClasses:
    @pytest.mark.parametrize(
        "raw",
        [_INTEGRATIONS_SUB, _WORKFLOWS_SUB, _HOSTED_EXTRACTORS_SUB],
        ids=["integrations", "workflows", "hosted-extractors"],
    )
    def test_request_round_trip(self, raw: dict) -> None:
        request = SignalSubscriptionRequest.model_validate(raw)
        assert request.dump(camel_case=True) == raw

    def test_as_id(self) -> None:
        request = SignalSubscriptionRequest.model_validate(_WORKFLOWS_SUB)
        assert request.as_id() == ExternalId(external_id="sub-wf")

    def test_as_update_only_sends_filter(self) -> None:
        request = SignalSubscriptionRequest.model_validate(_INTEGRATIONS_SUB)
        update = request.as_update(mode="replace")

        assert update["externalId"] == "sub-int"
        assert "sink" not in update.get("update", {})
        assert update["update"]["filter"]["set"]["topic"] == "cognite_integrations"

    def test_response_to_request(self) -> None:
        response = SignalSubscriptionResponse.model_validate(
            {**_WORKFLOWS_SUB, "createdTime": 1000, "lastUpdatedTime": 2000}
        )
        request = response.as_request_resource()
        assert isinstance(request, SignalSubscriptionRequest)
        assert request.external_id == "sub-wf"
        assert request.sink.type == "user"

    def test_unknown_filter_topic_preserved(self) -> None:
        raw = {
            "externalId": "sub-future",
            "sink": {"type": "current_user"},
            "filter": {"topic": "cognite_future_service", "someField": "abc"},
        }
        request = SignalSubscriptionRequest.model_validate(raw)
        assert isinstance(request.filter, UnknownSubscriptionFilter)
        assert request.filter.topic == "cognite_future_service"
        dumped = request.dump(camel_case=True)
        assert dumped["filter"]["topic"] == "cognite_future_service"
        assert dumped["filter"]["someField"] == "abc"


class TestSignalSubscriptionCRUDGetId:
    def test_get_id_from_dict(self) -> None:
        id_ = SignalSubscriptionIO.get_id({"externalId": "sub-1"})
        assert id_ == ExternalId(external_id="sub-1")

    def test_get_id_from_request(self) -> None:
        request = SignalSubscriptionRequest.model_validate(_INTEGRATIONS_SUB)
        id_ = SignalSubscriptionIO.get_id(request)
        assert id_ == ExternalId(external_id="sub-int")


class TestSignalSubscriptionCRUDGetDependencies:
    def test_email_sink_with_integration_resource(self) -> None:
        resource = SignalSubscriptionYAML.model_validate(_INTEGRATIONS_SUB)
        deps = list(SignalSubscriptionIO.get_dependencies(resource))
        assert deps == [(SignalSinkIO, SignalSinkId(type="email", external_id="sink-1"))]

    def test_user_sink_no_filter_resource(self) -> None:
        resource = SignalSubscriptionYAML.model_validate(_WORKFLOWS_SUB)
        deps = list(SignalSubscriptionIO.get_dependencies(resource))
        assert deps == [(SignalSinkIO, SignalSinkId(type="user", external_id="sink-2"))]

    def test_workflow_filter_resource(self) -> None:
        raw = {
            "externalId": "sub-wf-res",
            "sink": {"type": "user", "externalId": "sink-2"},
            "filter": {"topic": "cognite_workflows", "resource": "my-workflow"},
        }
        resource = SignalSubscriptionYAML.model_validate(raw)
        deps = list(SignalSubscriptionIO.get_dependencies(resource))
        assert (SignalSinkIO, SignalSinkId(type="user", external_id="sink-2")) in deps
        assert (WorkflowIO, ExternalId(external_id="my-workflow")) in deps
        assert len(deps) == 2

    def test_current_user_sink_no_dependency(self) -> None:
        raw = {
            "externalId": "sub-cu",
            "sink": {"type": "current_user"},
            "filter": {"topic": "cognite_workflows"},
        }
        resource = SignalSubscriptionYAML.model_validate(raw)
        deps = list(SignalSubscriptionIO.get_dependencies(resource))
        assert deps == []

    def test_hosted_extractor_filter_dependencies(self) -> None:
        resource = SignalSubscriptionYAML.model_validate(_HOSTED_EXTRACTORS_SUB)
        deps = list(SignalSubscriptionIO.get_dependencies(resource))
        assert (HostedExtractorSourceIO, ExternalId(external_id="my-source")) in deps
        assert (HostedExtractorDestinationIO, ExternalId(external_id="my-dest")) in deps
