import gzip
import json

import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.transformation_jobs import TransformationJobsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId

_EXAMPLE_JOB = {
    "id": 1,
    "uuid": "job-uuid-001",
    "transformationId": 205,
    "transformationExternalId": "transformation_001",
    "sourceProject": "my-project",
    "destinationProject": "my-project",
    "destination": {"type": "assets"},
    "conflictMode": "abort",
    "query": "SELECT * FROM source",
    "createdTime": 1622547800000,
    "startedTime": 1622547800000,
    "finishedTime": 1622547900000,
    "lastSeenTime": 1622547900000,
    "ignoreNullFields": True,
    "status": "Completed",
}

_EXAMPLE_METRIC = {"timestamp": 1622547800000, "name": "assets.read", "count": 10}


def _request_json(request: httpx2.Request) -> dict[str, object]:
    raw = request.content
    if len(raw) >= 2 and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


class TestTransformationJobsAPI:
    def test_retrieve_list_paginate_iterate_metrics(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationJobsAPI(client)

        retrieve_url = toolkit_config.create_api_url("/transformations/jobs/byids")
        respx_mock.post(retrieve_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_JOB]})
        )
        retrieved = api.retrieve([InternalId(id=1)], ignore_unknown_ids=True)
        assert len(retrieved) == 1
        assert retrieved[0].dump() == _EXAMPLE_JOB
        assert retrieved[0].as_id() == InternalId(id=1)
        assert _request_json(respx_mock.calls[-1].request) == {
            "items": [{"id": 1}],
            "ignoreUnknownIds": True,
        }

        list_url = toolkit_config.create_api_url("/transformations/jobs")
        respx_mock.get(list_url).mock(return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_JOB]}))
        listed = api.list(transformation_id=205, transformation_external_id="transformation_001", limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == _EXAMPLE_JOB
        list_params = dict(respx_mock.calls[-1].request.url.params)
        assert list_params["transformationId"] == "205"
        assert list_params["transformationExternalId"] == "transformation_001"
        assert list_params["limit"] == "10"

        page = api.paginate(transformation_id=205, limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == _EXAMPLE_JOB

        batches = list(api.iterate(transformation_external_id="transformation_001", limit=10))
        assert batches[0][0].dump() == _EXAMPLE_JOB

        metrics_url = toolkit_config.create_api_url("/transformations/jobs/1/metrics")
        respx_mock.get(metrics_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_METRIC]})
        )
        metrics = api.list_metrics(id=1)
        assert len(metrics) == 1
        assert metrics[0].dump() == _EXAMPLE_METRIC
