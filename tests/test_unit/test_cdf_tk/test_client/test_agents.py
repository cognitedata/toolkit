import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatAgentMessage,
    AgentChatResponse,
    AgentChatResult,
    AgentChatTextContent,
    AgentChatToolConfirmation,
    AgentChatToolConfirmationInfo,
    AgentChatToolConfirmationRequest,
    AgentChatUserMessage,
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
            ExternalId(external_id="my_agent"),
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
            ExternalId(external_id="my_agent"), [AgentChatUserMessage(content=AgentChatTextContent(text="Hi"))]
        )

        assert response == AgentChatResponse(
            agent_external_id="my_agent",
            response=AgentChatResult(
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
            ),
        )
