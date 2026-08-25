from typing import Annotated, Any, ClassVar, Literal

from pydantic import BeforeValidator, ConfigDict, Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class AgentObject(BaseModelObject):
    model_config = ConfigDict(extra="allow")


class AgentToolDefinition(AgentObject):
    model_config = ConfigDict(extra="allow")

    type: str
    name: str
    description: str


class AnalyzeImage(AgentToolDefinition):
    type: Literal["analyzeImage"] = "analyzeImage"


class AnalyzeTimeSeries(AgentToolDefinition):
    type: Literal["analyzeTimeSeries"] = "analyzeTimeSeries"


class AskDocument(AgentToolDefinition):
    type: Literal["askDocument"] = "askDocument"


class CallFunctionConfig(AgentObject):
    external_id: str
    max_polling_time: int = 540
    schema_: dict[str, Any] | None = Field(None, alias="schema")


class CallFunction(AgentToolDefinition):
    type: Literal["callFunction"] = "callFunction"
    configuration: CallFunctionConfig


class CallRestApi(AgentToolDefinition):
    type: Literal["callRestApi"] = "callRestApi"


class ExamineDataSemantically(AgentToolDefinition):
    type: Literal["examineDataSemantically"] = "examineDataSemantically"


class AgentDataModel(AgentObject):
    space: str
    external_id: str
    version: str
    view_external_ids: list[str] | None = None


class AgentInstanceSpacesDefinition(AgentObject):
    type: str


class AllInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["all"] = "all"


class ManualInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["manual"] = "manual"
    spaces: list[str]


class ProvidedAtRuntimeInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["providedAtRuntime"] = "providedAtRuntime"


class UnknownInstanceSpaces(AgentInstanceSpacesDefinition): ...


_KNOWN_INSTANCE_SPACES = {
    cls_.model_fields["type"].default: cls_
    for cls_ in get_concrete_subclasses(AgentInstanceSpacesDefinition)
    if cls_ is not UnknownInstanceSpaces
}


def _handle_unknown_instance_spaces(value: Any) -> Any:
    if isinstance(value, dict):
        type_ = value.get("type")
        if type_ not in _KNOWN_INSTANCE_SPACES:
            return UnknownInstanceSpaces(**value)
        else:
            return _KNOWN_INSTANCE_SPACES[type_].model_validate(value)
    return value


AgentInstanceSpaces = Annotated[
    AllInstanceSpaces | ManualInstanceSpaces | ProvidedAtRuntimeInstanceSpaces | UnknownInstanceSpaces,
    BeforeValidator(_handle_unknown_instance_spaces),
]


class ManualQueryDataModels(AgentObject):
    type: Literal["manual"] = "manual"
    data_models: list[AgentDataModel]


class ProvidedAtRuntimeQueryDataModels(AgentObject):
    type: Literal["providedAtRuntime"] = "providedAtRuntime"


class UnknownQueryDataModels(AgentObject):
    type: str


_KNOWN_QUERY_DATA_MODELS: dict[str, type[AgentObject]] = {
    "manual": ManualQueryDataModels,
    "providedAtRuntime": ProvidedAtRuntimeQueryDataModels,
}


def _handle_query_data_models(value: Any) -> Any:
    if isinstance(value, dict):
        type_ = value.get("type")
        if type_ not in _KNOWN_QUERY_DATA_MODELS:
            return UnknownQueryDataModels(**value)
        return _KNOWN_QUERY_DATA_MODELS[type_].model_validate(value)
    return value


QueryDataModels = Annotated[
    ManualQueryDataModels | ProvidedAtRuntimeQueryDataModels | UnknownQueryDataModels,
    BeforeValidator(_handle_query_data_models),
]


class QueryConfig(AgentObject):
    data_models: QueryDataModels
    instance_spaces: AgentInstanceSpaces | None = None


class Query(AgentToolDefinition):
    type: Literal["query"] = "query"
    configuration: QueryConfig


class QueryKnowledgeGraphConfig(AgentObject):
    data_models: list[AgentDataModel]
    instance_spaces: AgentInstanceSpaces | None = None
    # This is deviating from the API documentation, but the Atlas team has confirmed that "v2" is the default
    version: Literal["v1", "v2"] = "v2"


class QueryKnowledgeGraph(AgentToolDefinition):
    type: Literal["queryKnowledgeGraph"] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphConfig


class QueryTimeSeriesDatapoints(AgentToolDefinition):
    type: Literal["queryTimeSeriesDatapoints"] = "queryTimeSeriesDatapoints"


class RunPythonCode(AgentToolDefinition):
    type: Literal["runPythonCode"] = "runPythonCode"


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


class TimeSeriesAnalysis(AgentToolDefinition):
    type: Literal["timeSeriesAnalysis"] = "timeSeriesAnalysis"


class UnknownAgentTool(AgentToolDefinition):
    """Fallback for unknown tool types."""

    ...


KNOWN_TOOLS: dict[str, type[AgentToolDefinition]] = {
    "analyzeImage": AnalyzeImage,
    "analyzeTimeSeries": AnalyzeTimeSeries,
    "askDocument": AskDocument,
    "callFunction": CallFunction,
    "callRestApi": CallRestApi,
    "examineDataSemantically": ExamineDataSemantically,
    "query": Query,
    "queryKnowledgeGraph": QueryKnowledgeGraph,
    "queryTimeSeriesDatapoints": QueryTimeSeriesDatapoints,
    "runPythonCode": RunPythonCode,
    "summarizeDocument": SummarizeDocument,
    "timeSeriesAnalysis": TimeSeriesAnalysis,
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
    AnalyzeImage
    | AnalyzeTimeSeries
    | AskDocument
    | CallFunction
    | CallRestApi
    | ExamineDataSemantically
    | Query
    | QueryKnowledgeGraph
    | QueryTimeSeriesDatapoints
    | RunPythonCode
    | SummarizeDocument
    | TimeSeriesAnalysis
    | UnknownAgentTool,
    BeforeValidator(_handle_unknown_tool),
]


class SubagentConfig(AgentObject):
    agent_external_id: str = Field(min_length=1, max_length=255)


class ExampleMessage(AgentObject):
    role: str
    content: str


class ExampleQuestion(AgentObject):
    question: str
    expected_messages: list[ExampleMessage] = Field(default_factory=list)


class Agent(AgentObject):
    external_id: str
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str | None = None
    tools: list[AgentTool] | None = None
    subagents: list[SubagentConfig] | None = None
    skills: list[str] | None = None
    labels: list[str] | None = None
    example_questions: list[ExampleQuestion] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class AgentRequest(Agent, RequestResource):
    model_config = ConfigDict(extra="allow")
    runtime_version: str | None = None


class AgentResponse(Agent, ResponseResource[AgentRequest]):
    model_config = ConfigDict(extra="allow")
    created_time: int
    last_updated_time: int
    owner_id: str
    runtime_version: str

    @classmethod
    def request_cls(cls) -> type[AgentRequest]:
        return AgentRequest


class AgentRuntimeVersionInfo(AgentObject):
    """A single agent runtime version and the capabilities it supports."""

    version: str
    release_stage: Literal["stable", "preview"] | str
    visibility: Literal["hidden", "public"] | str | None = None
    capabilities: list[str] = Field(default_factory=list)


class AIServiceAvailability(AgentObject):
    """A single AI service (e.g. Agent CRUD, Chat completions) and its availability in the CDF project."""

    name: str
    path: str
    available: bool
    supported_language_models: list[str] = Field(default_factory=list)
    default_language_model: str | None = None
    default_advanced_language_model: str | None = None
    default_fast_language_model: str | None = None
    agent_runtime_versions: list[AgentRuntimeVersionInfo] = Field(default_factory=list)
    default_runtime_version: str | None = None
    additional_parameters: dict[str, Any] = Field(default_factory=dict)


class AILanguageModel(AgentObject):
    """A language model available in the CDF project, independent of which AI service supports it."""

    name: str
    native: bool
    max_tokens: int | None = None
    max_output_tokens: int | None = None
    earliest_retirement_date: int | None = None


class ServicesAvailability(AgentObject):
    """Response of the `/ai/services/availability` endpoint."""

    items: list[AIServiceAvailability]
    language_models: list[AILanguageModel]

    AGENT_CRUD_SERVICE_NAME: ClassVar[str] = "Agent CRUD"

    @property
    def agent_service(self) -> AIServiceAvailability | None:
        return next((item for item in self.items if item.name == self.AGENT_CRUD_SERVICE_NAME), None)

    @property
    def supported_agent_models(self) -> list[str] | None:
        agent_service = self.agent_service
        return agent_service.supported_language_models if agent_service else None

    @property
    def max_tools_per_agent(self) -> int | None:
        agent_service = self.agent_service
        if agent_service is None:
            return None
        limit = agent_service.additional_parameters.get("maxToolsPerAgentLimit")
        return limit if isinstance(limit, int) else None

    def runtime_version_has_capability(self, runtime_version: str, capability: str) -> bool | None:
        """Returns whether the given runtime version has the given capability, or None if unknown.

        Args:
            runtime_version: The agent runtime version to check, e.g. "1.3.0".
            capability: The capability to check for, e.g. "SUBAGENTS" or "SKILLS".
        """
        agent_service = self.agent_service
        if agent_service is None:
            return None
        for version_info in agent_service.agent_runtime_versions:
            if version_info.version == runtime_version:
                return capability in version_info.capabilities
        return None
