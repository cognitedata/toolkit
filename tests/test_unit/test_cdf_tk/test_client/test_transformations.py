import gzip
import json

import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.transformations import TransformationsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import NonceCredentials

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
    "ignoreNullFields": True,
    "status": "Created",
}


def _request_json(request: httpx2.Request) -> dict[str, object]:
    raw = request.content
    if len(raw) >= 2 and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


class TestTransformationsAPI:
    def test_run(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationsAPI(client)
        run_url = toolkit_config.create_api_url("/transformations/run")
        respx_mock.post(run_url).mock(return_value=httpx2.Response(status_code=200, json=_EXAMPLE_JOB))

        job = api.run(ExternalId(external_id="transformation_001"))
        assert job.dump() == _EXAMPLE_JOB
        assert _request_json(respx_mock.calls[-1].request) == {"externalId": "transformation_001"}

        nonce = NonceCredentials(session_id=42, nonce="session-nonce", cdf_project_name="my-project")
        job_with_nonce = api.run(InternalId(id=205), nonce=nonce)
        assert job_with_nonce.id == 1
        assert _request_json(respx_mock.calls[-1].request) == {
            "id": 205,
            "nonce": {"sessionId": 42, "nonce": "session-nonce", "cdfProjectName": "my-project"},
        }

    def test_run_query(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationsAPI(client)
        query_url = toolkit_config.create_api_url("/transformations/query/run")
        query_response = {
            "schema": {"items": [{"name": "col", "sqlType": "INT", "type": {"type": "integer"}, "nullable": True}]},
            "results": {"items": [{"col": 1}]},
        }
        respx_mock.post(query_url).mock(return_value=httpx2.Response(status_code=200, json=query_response))

        result = api.run_query(query="SELECT 1 AS col", convert_to_string=False, limit=10, source_limit=20)
        assert result.schema_[0].name == "col"
        assert result.results == [{"col": 1}]
        assert _request_json(respx_mock.calls[-1].request) == {
            "query": "SELECT 1 AS col",
            "convertToString": False,
            "limit": 10,
            "sourceLimit": 20,
            "inferSchemaLimit": 10_000,
            "timeout": TransformationsAPI.DEFAULT_TIMEOUT_RUN_QUERY,
        }
        assert TransformationsAPI.preview is TransformationsAPI.run_query
