import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExtractionPipelineConfigId
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    CogniteFormat,
    HostedExtractorJobRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_preview import HostedExtractorPreviewPayload


@pytest.mark.usefixtures("disable_gzip")
class TestExtractionPipelinesAndHostedExtractors:
    def test_extraction_pipeline_config_revert(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extpipes/config/revert")).mock(
            return_value=httpx2.Response(200, json={"externalId": "my_pipeline", "revision": 3, "createdTime": 0})
        )

        toolkit_client.extraction_pipelines.configs.revert(
            ExtractionPipelineConfigId(external_id="my_pipeline", revision=2)
        )

        assert json.loads(route.calls[0].request.content) == {"externalId": "my_pipeline", "revision": 2}

    def test_extraction_pipeline_runs_list(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extpipes/runs/list")).mock(
            return_value=httpx2.Response(200, json={"items": [{"id": 1, "status": "failure", "createdTime": 0}]})
        )

        toolkit_client.extraction_pipelines.runs.list(
            "my_pipeline", statuses=["failure"], message_substring="timeout", limit=10
        )

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"externalId": "my_pipeline", "statuses": ["failure"], "message": {"substring": "timeout"}},
            "limit": 10,
        }

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


def test_hosted_extractor_job_status_fields_only_sent_on_update() -> None:
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
