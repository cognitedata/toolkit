from typing import Annotated, Any, Literal

from pydantic import Field, ValidationInfo, field_validator

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning

from .base import BaseModelResource, ToolkitResource

# --- Agent tool definitions ---


class AgentToolDefinition(BaseModelResource, extra="allow"):
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


class AnalyzeImage(AgentToolDefinition):
    type: Literal["analyzeImage"] = "analyzeImage"


class AnalyzeTimeSeries(AgentToolDefinition):
    type: Literal["analyzeTimeSeries"] = "analyzeTimeSeries"


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


class CallRestApi(AgentToolDefinition):
    type: Literal["callRestApi"] = "callRestApi"


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


class RunPythonCode(AgentToolDefinition):
    type: Literal["runPythonCode"] = "runPythonCode"


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


class TimeSeriesAnalysis(AgentToolDefinition):
    type: Literal["timeSeriesAnalysis"] = "timeSeriesAnalysis"


_NON_GA_TOOL_TYPES: frozenset[str] = frozenset(
    {
        "analyzeImage",
        "analyzeTimeSeries",
        "callRestApi",
        "examineDataSemantically",
        "runPythonCode",
    }
)

_DEPRECATED_TOOL_TYPES: frozenset[str] = frozenset(
    {
        "timeSeriesAnalysis",
    }
)

KNOWN_TOOLS: frozenset[str] = frozenset(
    {
        "analyzeImage",
        "analyzeTimeSeries",
        "askDocument",
        "callFunction",
        "callRestApi",
        "examineDataSemantically",
        "queryKnowledgeGraph",
        "queryTimeSeriesDatapoints",
        "runPythonCode",
        "summarizeDocument",
        "timeSeriesAnalysis",
    }
)


AgentTool = Annotated[
    AnalyzeImage  # Alpha
    | AnalyzeTimeSeries  # Beta
    | AskDocument  # GA
    | CallFunction  # GA
    | CallRestApi  # Beta
    | ExamineDataSemantically
    | QueryKnowledgeGraph  # GA
    | QueryTimeSeriesDatapoints  # GA
    | RunPythonCode  # Beta
    | SummarizeDocument  # GA
    | TimeSeriesAnalysis,  # Deprecated
    Field(discriminator="type"),
]


# --- Agent YAML resource ---


Model = Literal[
    "azure/o3",
    "azure/o4-mini",
    "azure/gpt-4o",
    "azure/gpt-4o-mini",
    "azure/gpt-4.1",
    "azure/gpt-4.1-nano",
    "azure/gpt-4.1-mini",
    "azure/gpt-5",
    "azure/gpt-5-mini",
    "azure/gpt-5-nano",
    "azure/gpt-5.1",
    "gcp/claude-4.5-sonnet",
    "gcp/claude-4.5-haiku",
    "gcp/gemini-2.5-pro",
    "gcp/gemini-2.5-flash",
    "aws/claude-4.5-sonnet",
    "aws/claude-4.5-haiku",
    "aws/claude-4-sonnet",
    "aws/claude-3.5-sonnet",
]


class AgentYAML(ToolkitResource):
    """Atlas AI Agent"""

    external_id: str = Field(
        description="An external ID that uniquely identifies the agent.",
        min_length=1,
        max_length=128,
        pattern=r"^[^\x00]{1,128}$",
    )
    name: str = Field(
        description="A descriptive name intended for use in user interfaces.",
        min_length=1,
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="A human-readable description of what the agent does, used for documentation only. "
        "This description is not used by the language model.",
        max_length=1024,
    )
    instructions: str | None = Field(
        default=None,
        description="The instructions for the agent prompt the language model to understand "
        "the agent's goals and how to achieve them.",
        max_length=32000,
    )
    model: Model = Field(
        "azure/gpt-4o-mini", description="The name of the model to use. Defaults to your CDF project's default model."
    )
    tools: list[AgentTool] | None = Field(None, description="A list of tools available to the agent.", max_length=20)
    runtime_version: str | None = Field(None, description="The runtime version")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    @field_validator("tools", mode="before")
    @classmethod
    def _check_tool_types(cls, v: Any, info: ValidationInfo) -> Any:
        if not isinstance(v, list):
            return v

        warnings = info.context.get("warnings") if info.context else None
        if warnings is None:
            return v

        for tool in v:
            if not isinstance(tool, dict):
                continue
            tool_type = tool.get("type")
            if tool_type in _DEPRECATED_TOOL_TYPES:
                warnings.append(
                    MediumSeverityWarning(
                        f"Agent tool type {tool_type!r} is deprecated and may be removed in a future release."
                    )
                )
            elif tool_type in _NON_GA_TOOL_TYPES:
                warnings.append(
                    MediumSeverityWarning(
                        f"Agent tool type {tool_type!r} is not Generally Available and may change without notice."
                    )
                )
        return v
