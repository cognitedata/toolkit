import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ExtractorReleaseId

_EXTRACTOR = {"externalId": "db-extractor", "name": "DB extractor", "description": "Reads databases", "type": "global"}
_RELEASE = {"externalId": "db-extractor", "version": "1.2.3", "artifacts": []}


@pytest.mark.usefixtures("disable_gzip")
class TestExtractorsAPI:
    def test_list_truncates_client_side(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        other = {**_EXTRACTOR, "externalId": "other"}
        route = respx_mock.get(toolkit_config.create_api_url("/extractors")).mock(
            return_value=httpx2.Response(200, json={"items": [_EXTRACTOR, other]})
        )

        listed = toolkit_client.extractors.list(limit=1)

        assert ([item.as_id() for item in listed], dict(route.calls[0].request.url.params)) == (
            [ExternalId(external_id="db-extractor")],
            {},
        )

    def test_retrieve(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extractors/byids")).mock(
            return_value=httpx2.Response(200, json={"items": [_EXTRACTOR]})
        )

        toolkit_client.extractors.retrieve([ExternalId(external_id="db-extractor")], ignore_unknown_ids=True)

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"externalId": "db-extractor"}],
            "ignoreUnknownIds": True,
        }

    def test_retrieve_releases(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extractors/releases/byids")).mock(
            return_value=httpx2.Response(200, json={"items": [_RELEASE]})
        )

        toolkit_client.extractors.releases.retrieve([ExtractorReleaseId(external_id="db-extractor", version="1.2.3")])

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"externalId": "db-extractor", "version": "1.2.3"}],
            "ignoreUnknownIds": False,
        }

    def test_list_releases_by_extractor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/extractors/releases")).mock(
            return_value=httpx2.Response(200, json={"items": [_RELEASE]})
        )

        toolkit_client.extractors.releases.list(extractor_external_id="db-extractor")

        assert dict(route.calls[0].request.url.params) == {"externalId": "db-extractor"}

    def test_schema(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}
        respx_mock.get(toolkit_config.create_api_url("/extractors/schemas/db-extractor/1.2.3")).mock(
            return_value=httpx2.Response(200, json=schema)
        )

        assert toolkit_client.extractors.schema("db-extractor", "1.2.3") == schema
