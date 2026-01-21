from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class AgentToolDefinition(BaseModelObject):
    type: str
    name: str
    description: str


class AskDocument(AgentToolDefinition):
    type: Literal["askDocument"] = "askDocument"


class ExamineDataSemantically(AgentToolDefinition):
    type: Literal["examineDataSemantically"] = "examineDataSemantically"


class AgentDataModel(BaseModelObject):
    space: str
    external_id: str
    version: str
    view_external_ids: list[str] | None = None


class AgentInstanceSpacesDefinition(BaseModelObject):
    type: str


class AllInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["all"] = "all"


class ManualInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["manual"] = "manual"
    spaces: list[str]


AgentInstanceSpaces = Annotated[
    AllInstanceSpaces | ManualInstanceSpaces,
    Field(discriminator="type"),
]


class QueryKnowledgeGraphConfig(BaseModelObject):
    data_models: list[AgentDataModel]
    instance_spaces: AgentInstanceSpaces | None = None
    # This is deviating from the API documentation, but the Atlas team has confirmed that "v2" is the default
    version: Literal["v1", "v2"] = "v2"


class QueryKnowledgeGraph(AgentToolDefinition):
    type: Literal["queryKnowledgeGraph"] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphConfig


class QueryTimeSeriesDatapoints(AgentToolDefinition):
    type: Literal["queryTimeSeriesDatapoints"] = "queryTimeSeriesDatapoints"


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


class UnknownAgentTool(AgentToolDefinition):
    """Fallback for unknown tool types."""

    ...


# Mapping of known agent tool types to their classes
KNOWN_TOOLS: dict[str, type[AgentToolDefinition]] = {
    "askDocument": AskDocument,
    "examineDataSemantically": ExamineDataSemantically,
    "queryKnowledgeGraph": QueryKnowledgeGraph,
    "queryTimeSeriesDatapoints": QueryTimeSeriesDatapoints,
    "summarizeDocument": SummarizeDocument,
}


def _handle_unknown_tool(value: Any) -> Any:
    if isinstance(value, dict):
        tool_type = value.get("type")
        if tool_type not in KNOWN_TOOLS:
            return UnknownAgentTool(**value)
        else:
            return KNOWN_TOOLS[tool_type].model_validate(value)
    return value


AgentTool = Annotated[
    AskDocument
    | QueryKnowledgeGraph
    | QueryTimeSeriesDatapoints
    | SummarizeDocument
    | ExamineDataSemantically
    | UnknownAgentTool,
    BeforeValidator(_handle_unknown_tool),
]


class Agent(BaseModelObject):
    external_id: str
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str = "azure/gpt-4o-mini"
    tools: list[AgentTool] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class AgentRequest(Agent, RequestResource):
    runtime_version: str | None = None


class AgentResponse(Agent, ResponseResource[AgentRequest]):
    created_time: int
    last_updated_time: int
    owner_id: str
    runtime_version: str

    def as_request_resource(self) -> AgentRequest:
        return AgentRequest.model_validate(self.dump(), extra="ignore")
