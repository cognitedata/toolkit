import json
import sys
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, Field, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class AgentChatObject(BaseModelObject):
    model_config = ConfigDict(extra="allow")


class TextContent(AgentChatObject):
    type: Literal["text"] = "text"
    text: str = ""


def _normalize_text_content(value: Any) -> Any:
    if isinstance(value, str):
        return {"type": "text", "text": value}
    return value


MessageContent = Annotated[TextContent, BeforeValidator(_normalize_text_content)]


class ChatMessage(AgentChatObject):
    role: Literal["user"] = "user"
    content: MessageContent

    @classmethod
    def from_text(cls, text: str) -> Self:
        return cls(content=TextContent(text=text))


class ClientToolActionDefinition(AgentChatObject):
    name: str
    description: str
    parameters: dict[str, Any]


class ClientToolAction(AgentChatObject):
    type: Literal["clientTool"] = "clientTool"
    client_tool: ClientToolActionDefinition = Field(alias="clientTool")


class ChatActionResult(AgentChatObject):
    role: Literal["action"] = "action"
    type: Literal["clientTool"] = "clientTool"
    action_id: str
    content: MessageContent
    data: list[Any] = Field(default_factory=list)

    @classmethod
    def from_text(cls, action_id: str, text: str) -> Self:
        return cls(action_id=action_id, content=TextContent(text=text))


class ClientToolCallSpec(AgentChatObject):
    name: str
    arguments: str | dict[str, Any]

    def parsed_arguments(self) -> dict[str, Any]:
        if isinstance(self.arguments, str):
            return json.loads(self.arguments)
        return self.arguments


class ClientToolCall(AgentChatObject):
    type: Literal["clientTool"] = "clientTool"
    action_id: str
    client_tool: ClientToolCallSpec = Field(alias="clientTool")

    @property
    def name(self) -> str:
        return self.client_tool.name

    @property
    def arguments(self) -> dict[str, Any]:
        return self.client_tool.parsed_arguments()


class ToolConfirmationCall(AgentChatObject):
    type: Literal["toolConfirmation"] = "toolConfirmation"
    action_id: str
    tool_name: str
    tool_arguments: dict[str, Any] = Field(default_factory=dict)
    tool_description: str = ""
    tool_type: str = ""
    content: MessageContent | None = None


class ToolConfirmationResult(AgentChatObject):
    role: Literal["action"] = "action"
    type: Literal["toolConfirmation"] = "toolConfirmation"
    action_id: str
    status: Literal["ALLOW", "DENY"]


def _parse_action_call(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    action_type = value.get("type")
    if action_type == "clientTool":
        return ClientToolCall.model_validate(value)
    if action_type == "toolConfirmation":
        return ToolConfirmationCall.model_validate(value)
    return value


ActionCall = Annotated[
    ClientToolCall | ToolConfirmationCall | AgentChatObject,
    BeforeValidator(_parse_action_call),
]


class AgentChatMessage(AgentChatObject):
    role: Literal["agent"] = "agent"
    content: MessageContent | None = None
    actions: list[ActionCall] | None = None


class AgentChatResponseBody(AgentChatObject):
    type: str
    cursor: str | None = None
    messages: list[AgentChatMessage] = Field(default_factory=list)


class AgentChatResponse(AgentChatObject):
    agent_external_id: str
    response: AgentChatResponseBody

    @model_validator(mode="before")
    @classmethod
    def _accept_flat_response(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "response" in data:
            return data
        if "messages" in data:
            return {
                "agentExternalId": data.get("agentExternalId", ""),
                "response": {
                    "type": data.get("type", "result"),
                    "cursor": data.get("cursor"),
                    "messages": data.get("messages", []),
                },
            }
        return data

    @property
    def cursor(self) -> str | None:
        return self.response.cursor

    @property
    def text(self) -> str | None:
        for message in self.response.messages:
            if message.content and message.content.text:
                return message.content.text
        return None

    @property
    def action_calls(self) -> list[ClientToolCall]:
        calls: list[ClientToolCall] = []
        for message in self.response.messages:
            if not message.actions:
                continue
            for action in message.actions:
                if isinstance(action, ClientToolCall):
                    calls.append(action)
        return calls

    @property
    def tool_confirmation_calls(self) -> list[ToolConfirmationCall]:
        confirmations: list[ToolConfirmationCall] = []
        for message in self.response.messages:
            if not message.actions:
                continue
            for action in message.actions:
                if isinstance(action, ToolConfirmationCall):
                    confirmations.append(action)
        return confirmations
