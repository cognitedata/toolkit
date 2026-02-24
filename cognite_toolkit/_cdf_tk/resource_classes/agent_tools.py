from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field

from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN

from .base import BaseModelResource


class AgentToolDefinition(BaseModelResource):
    type: str
    name: str = Field(
        description="A name for the tool, unique within the agent.",
        min_length=1,
        max_length=64,
        pattern=r"^[^\x00]{1,64}$",
    )
    description: str = Field(
        description="A description of how the tool helps the language model understand when and how to use the tool.",
        min_length=10,
        max_length=1024,
    )


class AskDocument(AgentToolDefinition):
    type: Literal["askDocument"] = "askDocument"


class CallFunctionConfig(BaseModelResource):
    external_id: str = Field(
        description="The external id of an existing Cognite Function in your CDF project.",
        min_length=1,
        max_length=255,
    )
    max_polling_time: int = Field(
        default=540,
        description="The maximum time in seconds to poll for the Cognite Function to complete.",
        gt=0,
        lt=541,
    )
    schema_: dict[str, Any] = Field(
        alias="schema",
        description="The Cognite Function's params specified as a JSON schema.",
    )


class CallFunction(AgentToolDefinition):
    type: Literal["callFunction"] = "callFunction"
    configuration: CallFunctionConfig = Field(
        description="Configuration for the Call Function tool.",
    )


class ExamineDataSemantically(AgentToolDefinition):
    type: Literal["examineDataSemantically"] = "examineDataSemantically"


class AgentDataModel(BaseModelResource):
    space: str = Field(
        description="The space the data model is in.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="The external ID of the data model.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="The version of the data model.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    view_external_ids: list[str] = Field(description="The views of the data model.", min_length=1, max_length=10)


class AgentInstanceSpacesDefinition(BaseModelResource):
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


class QueryKnowledgeGraphConfig(BaseModelResource):
    data_models: list[AgentDataModel] = Field(
        description="List of relevant data models.",
        min_length=1,
        max_length=80,
    )
    instance_spaces: AgentInstanceSpaces
    version: Literal["v1", "v2"] = Field(
        "v2",
        description="The version of the query generation strategy to use. "
        "A higher number does not necessarily mean a better query.",
    )


class QueryKnowledgeGraph(AgentToolDefinition):
    type: Literal["queryKnowledgeGraph"] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphConfig = Field(
        description="Configuration for the Query Knowledge Graph tool.",
    )


class QueryTimeSeriesDatapoints(AgentToolDefinition):
    type: Literal["queryTimeSeriesDatapoints"] = "queryTimeSeriesDatapoints"


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


class UnknownAgentTool(AgentToolDefinition, extra="allow"):
    """Fallback for tool types not yet supported by the toolkit.

    Accepts arbitrary extra fields so that tools returned by the API
    can round-trip through dump/deploy without data loss.
    """

    ...


KNOWN_TOOLS: dict[str, type[AgentToolDefinition]] = {
    "askDocument": AskDocument,
    "callFunction": CallFunction,
    "examineDataSemantically": ExamineDataSemantically,
    "queryKnowledgeGraph": QueryKnowledgeGraph,
    "queryTimeSeriesDatapoints": QueryTimeSeriesDatapoints,
    "summarizeDocument": SummarizeDocument,
}


def _handle_unknown_tool(value: Any) -> Any:
    if isinstance(value, dict):
        tool_type = value.get("type")
        if tool_type not in KNOWN_TOOLS:
            return UnknownAgentTool.model_validate(value)
        return KNOWN_TOOLS[tool_type].model_validate(value)
    return value


AgentTool = Annotated[
    AskDocument
    | CallFunction
    | ExamineDataSemantically
    | QueryKnowledgeGraph
    | QueryTimeSeriesDatapoints
    | SummarizeDocument
    | UnknownAgentTool,
    BeforeValidator(_handle_unknown_tool),
]
