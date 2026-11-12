import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_jobs import HostedExtractorJobsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

_LOG = {"jobExternalId": "my_job", "type": "transform_error", "message": "Invalid JSON", "createdTime": 1730204346000}
_METRICS = {
    "jobExternalId": "my_job",
    "timestamp": 1730204346000,
    "sourceMessages": 10,
    "sourceBytes": 1024,
    "cdfInputValues": 8,
    "cdfRequests": 1,
    "transformFailures": 2,
    "cdfWriteFailures": 0,
    "cdfSkippedValues": 0,
    "cdfFailedValues": 0,
    "cdfUploadedValues": 8,
}
_DLQ_MESSAGE = {
    "key": "row1",
    "columns": {
        "timestamp": "2026-11-12T10:00:00Z",
        "jobId": 123,
        "error": "Expected a number",
        "errorCategory": "number_conversion",
        "decodedPayload": '{"value": "abc"}',
    },
}


class TestHostedExtractorJobsAPI:
    def test_list_logs_metrics_and_dlq_messages(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = HostedExtractorJobsAPI(HTTPClient(toolkit_config))
        respx_mock.get(toolkit_config.create_api_url("/hostedextractors/jobs/logs")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_LOG]})
        )
        respx_mock.get(toolkit_config.create_api_url("/hostedextractors/jobs/metrics")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_METRICS]})
        )
        respx_mock.get(toolkit_config.create_api_url("/hostedextractors/jobs/dlq")).mock(
            side_effect=[
                httpx2.Response(status_code=200, json={"items": [_DLQ_MESSAGE], "nextCursor": "next"}),
                httpx2.Response(status_code=200, json={"items": [_DLQ_MESSAGE]}),
            ]
        )

        logs = api.list_logs(job="my_job", limit=10)
        metrics = api.list_metrics(source="my_source")
        messages = api.list_dlq_messages(job="my_job", min_last_updated_time=0, limit=None)

        assert {
            "logs": [log.dump() for log in logs],
            "metrics": [metric.dump() for metric in metrics],
            "messages": [message.dump() for message in messages],
            "params": [dict(call.request.url.params) for call in respx_mock.calls],
        } == {
            "logs": [_LOG],
            "metrics": [_METRICS],
            "messages": [_DLQ_MESSAGE, _DLQ_MESSAGE],
            "params": [
                {"job": "my_job", "limit": "10"},
                {"source": "my_source", "limit": "100"},
                {"job": "my_job", "minLastUpdatedTime": "0", "limit": "1000"},
                {"job": "my_job", "minLastUpdatedTime": "0", "limit": "1000", "cursor": "next"},
            ],
        }
