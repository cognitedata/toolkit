import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentChatContent, AgentChatUserMessage


@pytest.mark.usefixtures("disable_gzip")
class TestAgentsAPI:
    def test_chat(
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
