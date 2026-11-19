import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatAgentMessage,
    AgentChatChunk,
    AgentChatCursor,
    AgentChatProgress,
    AgentChatResult,
    AgentChatStreamOptions,
    AgentChatTextContent,
    AgentChatToolConfirmation,
    AgentChatToolConfirmationInfo,
    AgentChatToolConfirmationRequest,
    AgentChatUserMessage,
    UnknownAgentChatResponse,
)

CHAT_RESPONSE = {
    "agentExternalId": "my_agent",
    "agentId": "123",
    "response": {
        "type": "result",
        "cursor": "abc",
        "messages": [
            {
                "role": "agent",
                "content": {"type": "text", "text": "May I run the function?"},
                "actions": [
                    {
                        "type": "toolConfirmation",
                        "actionId": "action_1",
                        "toolConfirmation": {
                            "content": {"type": "text", "text": "Run my_function"},
                            "toolName": "my_function",
                            "toolDescription": "Runs my function",
                            "toolType": "callFunction",
                            "toolArguments": {"x": 1},
                        },
                    }
                ],
            }
        ],
    },
}


@pytest.mark.usefixtures("disable_gzip")
class TestAgentsAPI:
    def test_chat_request(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/ai/agents/chat")).mock(
            return_value=httpx2.Response(200, json=CHAT_RESPONSE)
        )

        toolkit_client.agents.chat(
            "my_agent",
            [
                AgentChatUserMessage(content=AgentChatTextContent(text="Hi")),
                AgentChatToolConfirmation(action_id="action_1", status="ALLOW"),
            ],
            cursor="abc",
            content_type="application/json; charset=utf-8",
        )

        request = route.calls[0].request
        assert {
            "content-type": request.headers["content-type"],
            "body": json.loads(request.content),
        } == {
            "content-type": "application/json; charset=utf-8",
            "body": {
                "agentExternalId": "my_agent",
                "messages": [
                    {"role": "user", "content": {"type": "text", "text": "Hi"}},
                    {"role": "action", "type": "toolConfirmation", "actionId": "action_1", "status": "ALLOW"},
                ],
                "cursor": "abc",
            },
        }

    def test_chat_response(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/ai/agents/chat")).mock(
            return_value=httpx2.Response(200, json=CHAT_RESPONSE)
        )

        response = toolkit_client.agents.chat(
            "my_agent", [AgentChatUserMessage(content=AgentChatTextContent(text="Hi"))]
        )

        assert response.response == AgentChatResult(
            cursor="abc",
            messages=[
                AgentChatAgentMessage(
                    content=AgentChatTextContent(text="May I run the function?"),
                    actions=[
                        AgentChatToolConfirmationRequest(
                            action_id="action_1",
                            tool_confirmation=AgentChatToolConfirmationInfo(
                                content=AgentChatTextContent(text="Run my_function"),
                                tool_name="my_function",
                                tool_description="Runs my function",
                                tool_type="callFunction",
                                tool_arguments={"x": 1},
                            ),
                        )
                    ],
                )
            ],
        )

    def test_chat_stream(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        events = [
            {"type": "cursor", "cursor": "abc"},
            {"type": "progress", "content": "Searching"},
            {"type": "responseChunk", "content": "Hel"},
            {"type": "newEventType", "content": "lo"},
            {"type": "result", "cursor": "abc", "messages": []},
        ]
        route = respx_mock.post(toolkit_config.create_api_url("/ai/agents/chat")).mock(
            return_value=httpx2.Response(
                200,
                text="".join(
                    f"data: {json.dumps({'agentExternalId': 'my_agent', 'response': event})}\n\n" for event in events
                ),
                headers={"content-type": "text/event-stream"},
            )
        )

        responses = toolkit_client.agents.chat(
            "my_agent",
            [AgentChatUserMessage(content=AgentChatTextContent(text="Hi"))],
            stream=True,
            stream_options=AgentChatStreamOptions(return_cursor=True),
        )

        assert {
            "body": json.loads(route.calls[0].request.content),
            "responses": [response.response for response in responses],
        } == {
            "body": {
                "agentExternalId": "my_agent",
                "messages": [{"role": "user", "content": {"type": "text", "text": "Hi"}}],
                "stream": True,
                "streamOptions": {"returnCursor": True},
            },
            "responses": [
                AgentChatCursor(cursor="abc"),
                AgentChatProgress(content="Searching"),
                AgentChatChunk(content="Hel"),
                UnknownAgentChatResponse(type="newEventType", content="lo"),
                AgentChatResult(cursor="abc", messages=[]),
            ],
        }
