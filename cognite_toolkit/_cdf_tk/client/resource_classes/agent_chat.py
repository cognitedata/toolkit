"""Request and response classes for the agent chat endpoint.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/agent_session_ai_agents_chat_post

Note: This is an alpha API and may change in future releases.
"""

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import NodeUntypedId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentObject

AgentChatToolType: TypeAlias = (
    Literal[
        "analyzeData",
        "analyzeTimeSeries",
        "callFunction",
        "askDocument",
        "summarizeDocument",
        "runPythonCode",
        "analyzeImage",
        "examineDataSemantically",
        "queryKnowledgeGraph",
        "queryTimeSeriesDatapoints",
        "callRestApi",
        "callWebhook",
        "query",
        "timeSeriesAnalysis",
        "loadSkill",
        "customTool",
        "callSubAgent",
        "delete",
        "editFile",
        "execute",
        "glob",
        "grep",
        "ls",
        "presentFiles",
        "readFile",
        "writeFile",
        "writeTodos",
    ]
    | str
)


def _parse_by_type(known: Mapping[str, type[AgentObject]], unknown: type[AgentObject]) -> Callable[[Any], Any]:
    """Parses a dict into the class matching its 'type', falling back to the unknown class for new types."""

    def parse(value: Any) -> Any:
        if isinstance(value, dict):
            return known.get(value.get("type", ""), unknown).model_validate(value)
        return value

    return parse


class AgentChatTextContent(AgentObject):
    type: Literal["text"] = "text"
    text: str | None = None


class AgentChatImageContent(AgentObject):
    type: Literal["image"] = "image"
    data: str = Field(description="Base64-encoded image data.", max_length=6990508)
    media_type: Literal["image/png", "image/jpeg", "image/webp"] | str


AgentChatContentPart: TypeAlias = Annotated[AgentChatTextContent | AgentChatImageContent, Field(discriminator="type")]


class AgentChatView(AgentObject):
    space: str
    external_id: str
    version: str


class AgentChatPageReference(AgentObject):
    type: Literal["page"] = "page"
    page: int
    text: list[str] | None = None


class AgentChatDataDefinition(AgentObject):
    type: str


class AgentChatInstanceData(AgentChatDataDefinition):
    type: Literal["instance"] = "instance"
    view: AgentChatView
    instance_id: NodeUntypedId
    properties: dict[str, JsonValue] | None = None


class AgentChatTimeSeriesData(AgentChatDataDefinition):
    type: Literal["timeseries"] = "timeseries"
    view: AgentChatView
    instance_id: NodeUntypedId
    properties: dict[str, JsonValue] | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None


class AgentChatFileData(AgentChatDataDefinition):
    type: Literal["file"] = "file"
    view: AgentChatView
    instance_id: NodeUntypedId
    properties: dict[str, JsonValue] | None = None
    references: list[AgentChatPageReference] | None = None


class AgentChatSandboxFileData(AgentChatDataDefinition):
    type: Literal["sandbox-file"] = "sandbox-file"
    file_uri: str
    name: str
    mime_type: str
    size_bytes: int | None = None


class AgentChatClientData(AgentChatDataDefinition):
    type: Literal["client"] = "client"
    payload: JsonValue


class UnknownAgentChatData(AgentChatDataDefinition):
    """Fallback for data types not yet known to Toolkit."""

    ...


AgentChatData: TypeAlias = Annotated[
    AgentChatInstanceData
    | AgentChatTimeSeriesData
    | AgentChatFileData
    | AgentChatSandboxFileData
    | AgentChatClientData
    | UnknownAgentChatData,
    BeforeValidator(
        _parse_by_type(
            {
                "instance": AgentChatInstanceData,
                "timeseries": AgentChatTimeSeriesData,
                "file": AgentChatFileData,
                "sandbox-file": AgentChatSandboxFileData,
                "client": AgentChatClientData,
            },
            UnknownAgentChatData,
        )
    ),
]


class AgentChatUserMessage(AgentObject):
    """A message from the user to the agent."""

    role: Literal["user"] = "user"
    content: AgentChatTextContent | list[AgentChatContentPart]


class AgentChatClientToolResult(AgentObject):
    """The result of a client tool the agent asked the client to run."""

    role: Literal["action"] = "action"
    type: Literal["clientTool"] = "clientTool"
    action_id: str
    content: AgentChatTextContent
    data: list[AgentChatData] | None = None


class AgentChatToolConfirmation(AgentObject):
    """Allows or denies a tool call the agent asked confirmation for."""

    role: Literal["action"] = "action"
    type: Literal["toolConfirmation"] = "toolConfirmation"
    action_id: str
    status: Literal["ALLOW", "DENY"] | str


class AgentChatUserSession(AgentObject):
    """Answers the agent's request for a user session."""

    role: Literal["action"] = "action"
    type: Literal["userSession"] = "userSession"
    action_id: str
    nonce: str


AgentChatActionMessage: TypeAlias = Annotated[
    AgentChatClientToolResult | AgentChatToolConfirmation | AgentChatUserSession, Field(discriminator="type")
]
AgentChatMessage: TypeAlias = Annotated[AgentChatUserMessage | AgentChatActionMessage, Field(discriminator="role")]


class ClientToolProperty(AgentObject):
    """A JSON schema describing a client tool parameter."""

    type: Literal["object", "array", "boolean", "integer", "number", "string", "null"] | None = None
    title: str | None = None
    description: str | None = None
    format: str | None = None
    nullable: bool | None = None
    default: JsonValue | None = None
    enum: list[str] | None = None
    pattern: str | None = None
    minimum: int | None = None
    maximum: int | None = None
    exclusive_minimum: int | None = None
    exclusive_maximum: int | None = None
    min_length: int | None = None
    max_length: int | None = None
    min_items: int | None = None
    max_items: int | None = None
    items: "ClientToolProperty | None" = None
    properties: "dict[str, ClientToolProperty] | None" = None
    property_ordering: list[str] | None = None
    required: list[str] | None = None
    additional_properties: "bool | ClientToolProperty | None" = None
    any_of: "list[ClientToolProperty] | None" = None


class ClientToolParameters(AgentObject):
    type: Literal["object"] = "object"
    description: str | None = None
    properties: dict[str, ClientToolProperty] | None = None
    property_ordering: list[str] | None = None
    required: list[str] | None = None


class ClientToolSchema(AgentObject):
    name: str
    description: str | None = None
    parameters: ClientToolParameters


class AgentChatClientAction(AgentObject):
    """A tool the client offers the agent. The agent asks the client to run it through a client tool action."""

    type: Literal["clientTool"] = "clientTool"
    client_tool: ClientToolSchema


class AgentChatRequest(AgentObject):
    model_config = ConfigDict(validate_assignment=True)

    agent_external_id: str
    messages: list[AgentChatMessage] = Field(min_length=1, max_length=1000)
    cursor: str | None = None
    actions: list[AgentChatClientAction] | None = Field(None, max_length=20)
    retention_policy: Literal["temporary", "persisted"] | None = None
    tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None


class ToolConfirmationCaller(AgentObject):
    id: str
    name: str


class ClientToolCall(AgentObject):
    name: str
    arguments: str | None = Field(None, description="The tool arguments as a JSON string.")


class RestApiToolConfirmationDetails(AgentObject):
    endpoint_method: str
    endpoint_path: str


class AgentChatToolInfo(AgentObject):
    content: AgentChatTextContent
    tool_name: str
    tool_description: str
    tool_type: AgentChatToolType
    tool_arguments: dict[str, JsonValue]


class AgentChatToolConfirmationInfo(AgentChatToolInfo):
    caller: ToolConfirmationCaller | None = None
    details: RestApiToolConfirmationDetails | None = None


class AgentChatActionDefinition(AgentObject):
    type: str
    action_id: str


class AgentChatClientToolRequest(AgentChatActionDefinition):
    """The agent asks the client to run a client tool. Answer with an AgentChatClientToolResult."""

    type: Literal["clientTool"] = "clientTool"
    client_tool: ClientToolCall
    caller: ToolConfirmationCaller | None = None


class AgentChatToolConfirmationRequest(AgentChatActionDefinition):
    """The agent asks confirmation to run a tool. Answer with an AgentChatToolConfirmation."""

    type: Literal["toolConfirmation"] = "toolConfirmation"
    tool_confirmation: AgentChatToolConfirmationInfo


class AgentChatUserSessionRequest(AgentChatActionDefinition):
    """The agent asks for a user session. Answer with an AgentChatUserSession."""

    type: Literal["userSession"] = "userSession"
    user_session: AgentChatToolInfo


class UnknownAgentChatAction(AgentChatActionDefinition):
    """Fallback for action types not yet known to Toolkit."""

    ...


AgentChatAction: TypeAlias = Annotated[
    AgentChatClientToolRequest
    | AgentChatToolConfirmationRequest
    | AgentChatUserSessionRequest
    | UnknownAgentChatAction,
    BeforeValidator(
        _parse_by_type(
            {
                "clientTool": AgentChatClientToolRequest,
                "toolConfirmation": AgentChatToolConfirmationRequest,
                "userSession": AgentChatUserSessionRequest,
            },
            UnknownAgentChatAction,
        )
    ),
]


class ToolCallDetail(AgentObject):
    id: str
    name: str
    tool_type: AgentChatToolType
    input: dict[str, JsonValue]
    result: dict[str, JsonValue]


class AgentReasoningData(AgentObject):
    type: Literal["toolCall"] = "toolCall"
    tool_call: ToolCallDetail | None = None


class AgentReasoning(AgentObject):
    content: list[AgentChatTextContent]
    data: list[AgentReasoningData] | None = None


class AgentChatAgentMessage(AgentObject):
    """A message from the agent. Actions are client tool calls, tool confirmations, or user sessions it asks for."""

    role: Literal["agent"] = "agent"
    content: AgentChatTextContent
    actions: list[AgentChatAction] | None = None
    data: list[AgentChatData] | None = None
    reasoning: list[AgentReasoning] | None = None


class AgentChatResult(AgentObject):
    type: Literal["result"] = "result"
    cursor: str | None = None
    messages: list[AgentChatAgentMessage]


class AgentChatResponse(AgentObject):
    """The agent's reply. Pass ``response.cursor`` to the next chat call to continue the conversation."""

    agent_external_id: str
    response: AgentChatResult
