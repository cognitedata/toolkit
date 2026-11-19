import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.documents import (
    DocumentTableElement,
    DocumentTextElement,
    DocumentUnknownElement,
)


@pytest.mark.usefixtures("disable_gzip")
class TestDocumentsAPI:
    def test_search_passages_sends_default_expansion_strategy(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/documents/passages/search")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.documents.search_passages({"semanticSearch": {"property": ["content"], "value": "pump"}})

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"semanticSearch": {"property": ["content"], "value": "pump"}},
            "expansionStrategy": {"strategy": "symmetric", "chunk_count": 1},
            "limit": 10,
        }

    def test_elements_are_parsed_by_type(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        box = {"page": 1, "left": 0.0, "right": 1.0, "top": 0.0, "bottom": 1.0}
        respx_mock.post(toolkit_config.create_api_url("/documents/elements")).mock(
            return_value=httpx2.Response(
                200,
                json={
                    "elements": [
                        {"type": "title", "level": 1, "text": "Pump", **box},
                        {"type": "table", "rows": [[{"text": "cell", **box}]]},
                        {"type": "figure", "caption": "A pump"},
                    ]
                },
            )
        )

        elements = toolkit_client.documents.elements(InternalId(id=1), granularity="elements")

        assert [type(element) for element in elements] == [
            DocumentTextElement,
            DocumentTableElement,
            DocumentUnknownElement,
        ]
