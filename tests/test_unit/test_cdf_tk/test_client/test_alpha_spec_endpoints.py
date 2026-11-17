import json
from collections.abc import Callable, Iterable
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import (
    ExternalId,
    ExtractionPipelineConfigId,
    NodeId,
    ThreeDJobId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentChatContent, AgentChatUserMessage
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    CogniteFormat,
    HostedExtractorJobRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_preview import HostedExtractorPreviewPayload
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    CurrentUserSinkRef,
    SandboxSubscriptionFilter,
    SignalSubscriptionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import SimulationRunRequest
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import ThreeDRevisionClassicRequest

_SIMULATION_RUN = {
    "id": 1,
    "dataSetId": 2,
    "simulatorExternalId": "sim",
    "modelExternalId": "model",
    "modelRevisionExternalId": "model_rev",
    "routineExternalId": "routine",
    "routineRevisionExternalId": "routine_rev",
    "status": "ready",
    "runType": "external",
    "userId": "user",
    "logId": 3,
    "createdTime": 0,
    "lastUpdatedTime": 0,
}
_UNIT = {
    "externalId": "temperature:deg_c",
    "name": "DEG_C",
    "longName": "degree Celsius",
    "symbol": "°C",
    "aliasNames": ["C"],
    "quantity": "Temperature",
    "conversion": {"multiplier": 1.0, "offset": 273.15},
}
_JOB = ThreeDJobId(space="my_space", external_id="my_job")
_RESULT = NodeId(space="my_space", external_id="my_result")


def post_body_cases() -> Iterable[Any]:
    yield pytest.param(
        lambda client: client.transformations.cancel(ExternalId(external_id="my_transformation")),
        "/transformations/cancel",
        {},
        {"externalId": "my_transformation"},
        id="transformations.cancel",
    )
    yield pytest.param(
        lambda client: client.extraction_pipelines.configs.revert(
            ExtractionPipelineConfigId(external_id="my_pipeline", revision=2)
        ),
        "/extpipes/config/revert",
        {"externalId": "my_pipeline", "revision": 3, "createdTime": 0},
        {"externalId": "my_pipeline", "revision": 2},
        id="extraction_pipelines.configs.revert",
    )
    yield pytest.param(
        lambda client: client.extraction_pipelines.runs.list(
            "my_pipeline", statuses=["failure"], message_substring="timeout", limit=10
        ),
        "/extpipes/runs/list",
        {"items": [{"id": 1, "status": "failure", "createdTime": 0}]},
        {
            "filter": {"externalId": "my_pipeline", "statuses": ["failure"], "message": {"substring": "timeout"}},
            "limit": 10,
        },
        id="extraction_pipelines.runs.list",
    )
    yield pytest.param(
        lambda client: client.workflows.tasks.update("task-1", "COMPLETED", output={"answer": 42}),
        "/workflows/tasks/task-1/update",
        {"id": "task-1", "externalId": "my_task", "status": "COMPLETED"},
        {"status": "COMPLETED", "output": {"answer": 42}},
        id="workflows.tasks.update",
    )
    yield pytest.param(
        lambda client: client.signals.sync(CurrentUserSinkRef(), cursor="abc"),
        "/processes/signals/sync",
        {"items": [], "nextCursor": "def", "moreData": False},
        {"sink": {"type": "current_user"}, "limit": 100, "cursor": "abc"},
        id="signals.sync",
    )
    yield pytest.param(
        lambda client: client.agents.chat(
            ExternalId(external_id="my_agent"), [AgentChatUserMessage(content=AgentChatContent(text="Hi"))]
        ),
        "/ai/agents/chat",
        {
            "agentExternalId": "my_agent",
            "agentId": "123",
            "response": {
                "type": "result",
                "messages": [{"role": "agent", "content": {"type": "text", "text": "Hello"}}],
            },
        },
        {
            "agentExternalId": "my_agent",
            "messages": [{"role": "user", "content": {"type": "text", "text": "Hi"}}],
            "stream": False,
        },
        id="agents.chat",
    )
    yield pytest.param(
        lambda client: client.documents.search_passages({"semanticSearch": {"property": ["content"], "value": "pump"}}),
        "/documents/passages/search",
        {"items": []},
        {
            "filter": {"semanticSearch": {"property": ["content"], "value": "pump"}},
            "expansionStrategy": {"strategy": "symmetric", "chunk_count": 1},
            "limit": 10,
        },
        id="documents.search_passages",
    )
    yield pytest.param(
        lambda client: client.three_d.jobs.update_rejections(_JOB, _RESULT, add=["key1"]),
        "/3d/job/result/rejections",
        {},
        {
            "job": {"space": "my_space", "externalId": "my_job"},
            "result": {"instanceId": {"space": "my_space", "externalId": "my_result"}},
            "add": ["key1"],
        },
        id="three_d.jobs.update_rejections",
    )
    yield pytest.param(
        lambda client: client.three_d.revisions_classic.update_thumbnail(1, 2, file_id=3),
        "/3d/models/1/revisions/2/thumbnail",
        {},
        {"fileId": 3},
        id="three_d.revisions_classic.update_thumbnail",
    )
    yield pytest.param(
        lambda client: client.records.aggregate(
            "my_stream", {"total": {"count": {}}}, last_updated_time={"gte": "2d-ago"}
        ),
        "/streams/my_stream/records/aggregate",
        {"aggregates": {"total": {"count": 5}}},
        {"aggregates": {"total": {"count": {}}}, "lastUpdatedTime": {"gte": "2d-ago"}},
        id="records.aggregate",
    )
    yield pytest.param(
        lambda client: client.datasets.count(external_id_prefix="ds_"),
        "/datasets/aggregate",
        {"items": [{"count": 4}]},
        {"filter": {"externalIdPrefix": "ds_"}},
        id="datasets.count",
    )
    yield pytest.param(
        lambda client: client.simulators.models.count(),
        "/simulators/models/aggregate",
        {"items": [{"count": 4}]},
        {"aggregate": "count"},
        id="simulators.models.count",
    )
    yield pytest.param(
        lambda client: client.simulators.runs.create([SimulationRunRequest(routine_external_id="routine")]),
        "/simulators/run",
        {"items": [_SIMULATION_RUN]},
        {"items": [{"routineExternalId": "routine"}]},
        id="simulators.runs.create",
    )
    yield pytest.param(
        lambda client: client.units.retrieve([ExternalId(external_id="temperature:deg_c")], ignore_unknown_ids=True),
        "/units/byids",
        {"items": [_UNIT]},
        {"items": [{"externalId": "temperature:deg_c"}], "ignoreUnknownIds": True},
        id="units.retrieve",
    )
    yield pytest.param(
        lambda client: client.instances.inspect([NodeId(space="my_space", external_id="my_node")], all_versions=True),
        "/models/instances/inspect",
        {"items": []},
        {
            "items": [{"instanceType": "node", "space": "my_space", "externalId": "my_node"}],
            "inspectionOperations": {
                "involvedViews": {"allVersions": True},
                "involvedContainers": {"allVersions": True},
            },
        },
        id="instances.inspect",
    )
    yield pytest.param(
        lambda client: client.annotations.reverse_lookup("file", annotation_type="images.AssetLink"),
        "/annotations/reverselookup",
        {"items": [{"id": 1}]},
        {"filter": {"annotatedResourceType": "file", "annotationType": "images.AssetLink"}, "limit": 100},
        id="annotations.reverse_lookup",
    )


def get_params_cases() -> Iterable[Any]:
    yield pytest.param(
        lambda client: client.workflows.triggers.history(ExternalId(external_id="my_trigger"), limit=5),
        "/workflows/triggers/my_trigger/history",
        {"items": []},
        {"limit": "5"},
        id="workflows.triggers.history",
    )
    yield pytest.param(
        lambda client: client.three_d.revisions_classic.list_logs(1, 2, severity=7),
        "/3d/models/1/revisions/2/logs",
        {"items": []},
        {"severity": "7"},
        id="three_d.revisions_classic.list_logs",
    )
    yield pytest.param(
        lambda client: client.raw.tables.rows.cursors("my_db", "my_table", number_of_cursors=2),
        "/raw/dbs/my_db/tables/my_table/cursors",
        {"items": ["c1", "c2"]},
        {"numberOfCursors": "2"},
        id="raw.tables.rows.cursors",
    )
    yield pytest.param(
        lambda client: client.three_d.asset_mappings_classic.list_by_asset(123, limit=10),
        "/3d/mappings/123/modelnodes",
        {"items": []},
        {"limit": "10"},
        id="three_d.asset_mappings_classic.list_by_asset",
    )
    yield pytest.param(
        lambda client: client.filemetadata.icon(ExternalId(external_id="my_image")),
        "/files/icon",
        {},
        {"externalId": "my_image"},
        id="filemetadata.icon",
    )


@pytest.mark.usefixtures("disable_gzip")
class TestAlphaSpecEndpoints:
    @pytest.mark.parametrize("call, path, response_body, expected_body", list(post_body_cases()))
    def test_request_body(
        self,
        call: Callable[[ToolkitClient], Any],
        path: str,
        response_body: dict[str, Any],
        expected_body: dict[str, Any],
        toolkit_client: ToolkitClient,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url(path)).mock(
            return_value=httpx2.Response(200, json=response_body)
        )

        call(toolkit_client)

        assert json.loads(route.calls[0].request.content) == expected_body

    @pytest.mark.parametrize("call, path, response_body, expected_params", list(get_params_cases()))
    def test_request_params(
        self,
        call: Callable[[ToolkitClient], Any],
        path: str,
        response_body: dict[str, Any],
        expected_params: dict[str, str],
        toolkit_client: ToolkitClient,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url(path)).mock(
            return_value=httpx2.Response(200, json=response_body)
        )

        call(toolkit_client)

        assert dict(route.calls[0].request.url.params) == expected_params

    def test_signals_list_follows_cursor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        signal = {
            "externalId": "s1",
            "topic": "cognite_workflows",
            "sink": {"type": "current_user"},
            "severity": "info",
            "resourceId": "my_workflow",
            "category": "execution_failed",
            "metadata": {},
            "message": "The workflow failed.",
        }
        route = respx_mock.post(toolkit_config.create_api_url("/processes/signals/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [signal], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{**signal, "externalId": "s2"}]}),
            ]
        )

        result = toolkit_client.signals.list(CurrentUserSinkRef(), limit=None)

        assert ([item.external_id for item in result], json.loads(route.calls[1].request.content)) == (
            ["s1", "s2"],
            {"sink": {"type": "current_user"}, "limit": 1000, "cursor": "c1"},
        )

    def test_three_d_job_rejections_truncated_to_limit(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/3d/job/result/rejections/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [{"rejectedKey": "k1"}], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{"rejectedKey": "k2"}, {"rejectedKey": "k3"}]}),
            ]
        )

        assert toolkit_client.three_d.jobs.list_rejections(_JOB, _RESULT, limit=2) == ["k1", "k2"]

    def test_three_d_job_update_rejections_requires_keys(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="at least one key"):
            toolkit_client.three_d.jobs.update_rejections(_JOB, _RESULT)

    def test_hosted_extractor_preview_result_is_parsed_by_type(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/hostedextractors/preview/result")).mock(
            return_value=httpx2.Response(
                200, json={"type": "preview", "externalId": "my_preview", "json": {"value": 1}, "createdTime": 0}
            )
        )

        result = toolkit_client.hosted_extractors.previews.result("my_preview")

        assert result == HostedExtractorPreviewPayload(external_id="my_preview", payload={"value": 1}, created_time=0)


class TestUpdateOnlyAndCreateOnlyFields:
    def test_hosted_extractor_job_status_fields_only_sent_on_update(self) -> None:
        job = HostedExtractorJobRequest(
            external_id="my_job",
            destination_id="my_destination",
            source_id="my_source",
            format=CogniteFormat(),
            target_status="paused",
            dlq_enabled=True,
        )

        update = job.as_update(mode="patch")["update"]

        assert (set(job.dump()) & {"targetStatus", "dlqEnabled"}, update["targetStatus"], update["dlqEnabled"]) == (
            set(),
            {"set": "paused"},
            {"set": True},
        )

    def test_three_d_revision_create_only_fields_not_updated(self) -> None:
        revision = ThreeDRevisionClassicRequest(
            model_id=1,
            id=2,
            file_id=3,
            nonce="my_nonce",
            pipeline_configuration={"indexingOptions": {}},
            published=True,
        )

        assert (
            revision.as_update(mode="replace")["update"].keys() & {"fileId", "nonce", "pipelineConfiguration"} == set()
        )

    def test_signal_subscription_parses_sandbox_filter(self) -> None:
        subscription = SignalSubscriptionResponse.model_validate(
            {
                "externalId": "my_subscription",
                "sink": {"type": "current_user"},
                "filter": {"topic": "cognite_sandbox", "tag": "my_tag"},
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        )

        assert subscription.filter == SandboxSubscriptionFilter(topic="cognite_sandbox", tag="my_tag")
