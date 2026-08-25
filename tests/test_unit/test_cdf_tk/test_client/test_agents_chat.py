import json
from unittest.mock import MagicMock

import respx
from httpx import Response
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.agents import AgentsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatResponse,
    ChatActionResult,
    ChatMessage,
    ClientToolAction,
    ClientToolActionDefinition,
)

CHAT_RESPONSE_JSON = {
    "agentExternalId": "cdf_toolkit_cli",
    "response": {
        "type": "result",
        "cursor": "cursor-1",
        "messages": [
            {
                "role": "agent",
                "content": {"type": "text", "text": "Here is the answer."},
                "actions": [
                    {
                        "type": "clientTool",
                        "actionId": "call-1",
                        "clientTool": {
                            "name": "read_file",
                            "arguments": json.dumps({"path": "cdf.toml"}),
                        },
                    }
                ],
            }
        ],
    },
}


class TestAgentChatModels:
    def test_parse_chat_response(self) -> None:
        response = AgentChatResponse.model_validate(CHAT_RESPONSE_JSON)
        assert response.agent_external_id == "cdf_toolkit_cli"
        assert response.cursor == "cursor-1"
        assert response.text == "Here is the answer."
        assert len(response.action_calls) == 1
        call = response.action_calls[0]
        assert call.name == "read_file"
        assert call.arguments == {"path": "cdf.toml"}

    def test_chat_message_dump(self) -> None:
        dumped = ChatMessage.from_text("hello").dump()
        assert dumped == {"role": "user", "content": {"type": "text", "text": "hello"}}

    def test_action_result_dump(self) -> None:
        dumped = ChatActionResult.from_text("call-1", "done").dump()
        assert dumped["role"] == "action"
        assert dumped["type"] == "clientTool"
        assert dumped["actionId"] == "call-1"


class TestAgentsAPIChat:
    def test_chat_posts_expected_body(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        http_client = HTTPClient(toolkit_config, console=MagicMock(spec=Console))
        api = AgentsAPI(http_client)
        url = toolkit_config.create_api_url("/ai/agents/chat")

        route = respx_mock.post(url).mock(return_value=Response(200, json=CHAT_RESPONSE_JSON))

        action = ClientToolAction(
            client_tool=ClientToolActionDefinition(
                name="read_file",
                description="Read a file",
                parameters={"type": "object", "properties": {"path": {"type": "string"}}},
            )
        )
        response = api.chat(
            "cdf_toolkit_cli",
            ChatMessage.from_text("list modules"),
            actions=[action],
        )

        assert response.text == "Here is the answer."
        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body["agentExternalId"] == "cdf_toolkit_cli"
        assert body["messages"][0]["content"]["text"] == "list modules"
        assert body["actions"][0]["type"] == "clientTool"
        assert body["actions"][0]["clientTool"]["name"] == "read_file"

    def test_chat_continues_with_cursor(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        http_client = HTTPClient(toolkit_config, console=MagicMock(spec=Console))
        api = AgentsAPI(http_client)
        url = toolkit_config.create_api_url("/ai/agents/chat")

        final_response = {
            "agentExternalId": "cdf_toolkit_cli",
            "response": {
                "type": "result",
                "cursor": "cursor-2",
                "messages": [
                    {
                        "role": "agent",
                        "content": {"type": "text", "text": "Finished."},
                    }
                ],
            },
        }
        route = respx_mock.post(url).mock(return_value=Response(200, json=final_response))

        response = api.chat(
            "cdf_toolkit_cli",
            ChatActionResult.from_text("call-1", "file contents"),
            cursor="cursor-1",
        )

        assert response.text == "Finished."
        body = json.loads(route.calls.last.request.content.decode())
        assert body["cursor"] == "cursor-1"
        assert body["messages"][0]["role"] == "action"
