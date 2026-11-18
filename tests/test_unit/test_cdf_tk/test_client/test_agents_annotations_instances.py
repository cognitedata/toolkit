import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentChatContent, AgentChatUserMessage


@pytest.mark.usefixtures("disable_gzip")
class TestAgentsAnnotationsInstances:
    def test_agents_chat(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/ai/agents/chat")).mock(
            return_value=httpx2.Response(
                200,
                json={
                    "agentExternalId": "my_agent",
                    "agentId": "123",
                    "response": {
                        "type": "result",
                        "messages": [{"role": "agent", "content": {"type": "text", "text": "Hello"}}],
                    },
                },
            )
        )

        toolkit_client.agents.chat(
            ExternalId(external_id="my_agent"), [AgentChatUserMessage(content=AgentChatContent(text="Hi"))]
        )

        assert json.loads(route.calls[0].request.content) == {
            "agentExternalId": "my_agent",
            "messages": [{"role": "user", "content": {"type": "text", "text": "Hi"}}],
            "stream": False,
        }

    def test_annotations_reverse_lookup(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/annotations/reverselookup")).mock(
            return_value=httpx2.Response(200, json={"items": [{"id": 1}]})
        )

        toolkit_client.annotations.reverse_lookup("file", annotation_type="images.AssetLink")

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"annotatedResourceType": "file", "annotationType": "images.AssetLink"},
            "limit": 100,
        }

    def test_instances_inspect(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/models/instances/inspect")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.instances.inspect([NodeId(space="my_space", external_id="my_node")], all_versions=True)

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"instanceType": "node", "space": "my_space", "externalId": "my_node"}],
            "inspectionOperations": {
                "involvedViews": {"allVersions": True},
                "involvedContainers": {"allVersions": True},
            },
        }
