from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SignalSinkId, SignalSubscriptionId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SignalSubscriptionRequest,
    SignalSubscriptionResponse,
)
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._resource_cruds.signal_sink import SignalSinkCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.signal_subscription import SignalSubscriptionCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.workflow import WorkflowCRUD
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
            "does not match any of the expected tags: 'cognite_integrations', 'cognite_workflows'",
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
        assert loaded.as_id() == SignalSubscriptionId(external_id="sub-wf")


class TestSignalSubscriptionResourceClasses:
    @pytest.mark.parametrize("raw", [_INTEGRATIONS_SUB, _WORKFLOWS_SUB], ids=["integrations", "workflows"])
    def test_request_round_trip(self, raw: dict) -> None:
        request = SignalSubscriptionRequest.model_validate(raw)
        assert request.dump(camel_case=True) == raw

    def test_as_id(self) -> None:
        request = SignalSubscriptionRequest.model_validate(_WORKFLOWS_SUB)
        assert request.as_id() == SignalSubscriptionId(external_id="sub-wf")

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


class TestSignalSubscriptionCRUDGetId:
    def test_get_id_from_dict(self) -> None:
        id_ = SignalSubscriptionCRUD.get_id({"externalId": "sub-1"})
        assert id_ == SignalSubscriptionId(external_id="sub-1")

    def test_get_id_from_request(self) -> None:
        request = SignalSubscriptionRequest.model_validate(_INTEGRATIONS_SUB)
        id_ = SignalSubscriptionCRUD.get_id(request)
        assert id_ == SignalSubscriptionId(external_id="sub-int")


class TestSignalSubscriptionCRUDGetDependencies:
    def test_email_sink_with_integration_resource(self) -> None:
        resource = SignalSubscriptionYAML.model_validate(_INTEGRATIONS_SUB)
        deps = list(SignalSubscriptionCRUD.get_dependencies(resource))
        assert deps == [(SignalSinkCRUD, SignalSinkId(type="email", external_id="sink-1"))]

    def test_user_sink_no_filter_resource(self) -> None:
        resource = SignalSubscriptionYAML.model_validate(_WORKFLOWS_SUB)
        deps = list(SignalSubscriptionCRUD.get_dependencies(resource))
        assert deps == [(SignalSinkCRUD, SignalSinkId(type="user", external_id="sink-2"))]

    def test_workflow_filter_resource(self) -> None:
        raw = {
            "externalId": "sub-wf-res",
            "sink": {"type": "user", "externalId": "sink-2"},
            "filter": {"topic": "cognite_workflows", "resource": "my-workflow"},
        }
        resource = SignalSubscriptionYAML.model_validate(raw)
        deps = list(SignalSubscriptionCRUD.get_dependencies(resource))
        assert (SignalSinkCRUD, SignalSinkId(type="user", external_id="sink-2")) in deps
        assert (WorkflowCRUD, ExternalId(external_id="my-workflow")) in deps
        assert len(deps) == 2

    def test_current_user_sink_no_dependency(self) -> None:
        raw = {
            "externalId": "sub-cu",
            "sink": {"type": "current_user", "externalId": "me"},
            "filter": {"topic": "cognite_workflows"},
        }
        resource = SignalSubscriptionYAML.model_validate(raw)
        deps = list(SignalSubscriptionCRUD.get_dependencies(resource))
        assert deps == []
