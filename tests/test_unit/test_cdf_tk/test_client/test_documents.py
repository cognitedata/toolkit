import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.documents import (
    DocumentSummary,
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

    def test_ask_sends_question_and_parses_answer(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        location = {"pageNumber": 2, "left": 0.1, "right": 0.9, "top": 0.2, "bottom": 0.3}
        route = respx_mock.post(toolkit_config.create_api_url("/ai/tools/documents/ask")).mock(
            return_value=httpx2.Response(
                200,
                json={
                    "content": [
                        {
                            "text": "The pump runs at 50 Hz.",
                            "references": [{"fileId": 1, "fileName": "pump.pdf", "locations": [location]}],
                        }
                    ]
                },
            )
        )

        answer = toolkit_client.documents.ask(
            "What frequency does the pump run at?",
            [InternalId(id=1), ExternalId(external_id="manual")],
            language="English",
        )

        assert (json.loads(route.calls[0].request.content), answer.dump()) == (
            {
                "question": "What frequency does the pump run at?",
                "fileIds": [{"id": 1}, {"externalId": "manual"}],
                "ignoreUnknownIds": False,
                "language": "English",
            },
            {
                "content": [
                    {
                        "text": "The pump runs at 50 Hz.",
                        "references": [{"fileId": 1, "fileName": "pump.pdf", "locations": [location]}],
                    }
                ]
            },
        )

    def test_summarize_sends_one_document_per_request(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/ai/tools/documents/summarize")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [{"id": 1, "summary": "About pumps."}]}),
                httpx2.Response(200, json={"items": [{"externalId": "manual", "summary": "About valves."}]}),
            ]
        )

        summaries = toolkit_client.documents.summarize([InternalId(id=1), ExternalId(external_id="manual")])

        assert ([json.loads(call.request.content) for call in route.calls], summaries) == (
            [
                {"items": [{"id": 1}], "ignoreUnknownIds": False},
                {"items": [{"externalId": "manual"}], "ignoreUnknownIds": False},
            ],
            [
                DocumentSummary(id=1, summary="About pumps."),
                DocumentSummary(external_id="manual", summary="About valves."),
            ],
        )
