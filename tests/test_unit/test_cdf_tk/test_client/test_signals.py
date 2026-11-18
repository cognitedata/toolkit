import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.signal import (
    CommentNotificationsSignalMetadata,
    HostedExtractorsSignalMetadata,
    IntegrationSignalMetadata,
    SandboxSignalMetadata,
    SignalMetadata,
    SignalResponse,
    UnknownSignalMetadata,
    WorkflowSignalMetadata,
)
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import CurrentUserSinkRef

_SIGNAL = {
    "externalId": "s1",
    "topic": "cognite_workflows",
    "sink": {"type": "current_user"},
    "severity": "info",
    "resourceId": "my_workflow",
    "category": "execution_failed",
    "metadata": {},
    "message": "The workflow failed.",
}


@pytest.mark.parametrize(
    "topic, metadata, expected_cls",
    [
        pytest.param(
            "cognite_integrations",
            {"extractorExternalId": "ext", "taskName": "t", "startTime": 1, "endTime": 2},
            IntegrationSignalMetadata,
            id="integrations",
        ),
        pytest.param(
            "cognite_workflows",
            {"executionStartTime": "a", "triggerExternalId": "trigger"},
            WorkflowSignalMetadata,
            id="workflows",
        ),
        pytest.param(
            "cognite_hosted_extractors",
            {"sourceType": "mqtt", "sourceExternalId": "src"},
            HostedExtractorsSignalMetadata,
            id="hosted-extractors",
        ),
        pytest.param("cognite_sandbox", {"tag": "my_tag", "detail": "d"}, SandboxSignalMetadata, id="sandbox"),
        pytest.param(
            "cognite_comment_notifications",
            {"activityName": "a", "planId": "p"},
            CommentNotificationsSignalMetadata,
            id="comment-notifications",
        ),
        pytest.param("cognite_future", {"newField": "x"}, UnknownSignalMetadata, id="unknown-topic"),
    ],
)
def test_signal_metadata_parsed_by_topic(
    topic: str, metadata: dict[str, object], expected_cls: type[SignalMetadata]
) -> None:
    raw = {**_SIGNAL, "topic": topic, "metadata": metadata}

    signal = SignalResponse.model_validate(raw)

    assert (type(signal.metadata), signal.dump()) == (expected_cls, raw)


@pytest.mark.usefixtures("disable_gzip")
class TestSignalsAPI:
    def test_sync_sends_sink_and_cursor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/processes/signals/sync")).mock(
            return_value=httpx2.Response(200, json={"items": [], "nextCursor": "def", "moreData": False})
        )

        toolkit_client.signals.sync(CurrentUserSinkRef(), cursor="abc")

        assert json.loads(route.calls[0].request.content) == {
            "sink": {"type": "current_user"},
            "limit": 100,
            "cursor": "abc",
        }

    def test_list_follows_cursor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/processes/signals/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [_SIGNAL], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{**_SIGNAL, "externalId": "s2"}]}),
            ]
        )

        result = toolkit_client.signals.list(CurrentUserSinkRef(), limit=None)

        assert ([item.external_id for item in result], json.loads(route.calls[1].request.content)) == (
            ["s1", "s2"],
            {"sink": {"type": "current_user"}, "limit": 1000, "cursor": "c1"},
        )
