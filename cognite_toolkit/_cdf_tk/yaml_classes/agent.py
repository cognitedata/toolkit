import json
import sys
from typing import Annotated, Any, Literal

from pydantic import Field, field_validator, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

MAX_SUB_AGENTS_PER_AGENT = 20
EXAMPLE_QUESTIONS_MAX_LENGTH = 5
EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE = 40960
RUNTIME_VERSIONS_SUPPORTING_SUBAGENTS = frozenset({"1.3.0", "1.4.0"})


class AgentToolModelResource(BaseModelResource, extra="allow"): ...


# --- Agent tool definitions ---


class AgentToolDefinition(AgentToolModelResource):
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


class CallFunctionConfig(AgentToolModelResource):
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


class AgentDataModel(AgentToolModelResource):
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


class AgentInstanceSpacesDefinition(AgentToolModelResource):
    type: str


class AllInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["all"] = "all"


class ManualInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["manual"] = "manual"
    spaces: list[str]


class ProvidedAtRuntimeInstanceSpaces(AgentInstanceSpacesDefinition):
    type: Literal["providedAtRuntime"] = "providedAtRuntime"


AgentInstanceSpaces = Annotated[
    AllInstanceSpaces | ManualInstanceSpaces | ProvidedAtRuntimeInstanceSpaces,
    Field(discriminator="type"),
]


class ManualQueryDataModels(AgentToolModelResource):
    type: Literal["manual"] = "manual"
    data_models: list[AgentDataModel] = Field(
        description="List of relevant data models.",
        min_length=1,
        max_length=80,
    )


class ProvidedAtRuntimeQueryDataModels(AgentToolModelResource):
    type: Literal["providedAtRuntime"] = "providedAtRuntime"


QueryDataModels = Annotated[
    ManualQueryDataModels | ProvidedAtRuntimeQueryDataModels,
    Field(discriminator="type"),
]


class QueryConfig(AgentToolModelResource):
    data_models: QueryDataModels = Field(description="Data model scope for the Query tool.")
    instance_spaces: AgentInstanceSpaces = Field(description="Instance space scope for the Query tool.")


class Query(AgentToolDefinition):
    type: Literal["query"] = "query"
    configuration: QueryConfig = Field(description="Configuration for the Query tool.")


class QueryKnowledgeGraphConfig(AgentToolModelResource):
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
    configuration: dict[str, Any] | None = Field(
        default=None,
        description="Configuration for the Run Python Code tool.",
    )


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


class TimeSeriesAnalysis(AgentToolDefinition):
    type: Literal["timeSeriesAnalysis"] = "timeSeriesAnalysis"


KNOWN_TOOLS: frozenset[str] = frozenset(
    {
        "analyzeImage",
        "analyzeTimeSeries",
        "askDocument",
        "callFunction",
        "callRestApi",
        "examineDataSemantically",
        "query",
        "queryKnowledgeGraph",
        "queryTimeSeriesDatapoints",
        "runPythonCode",
        "summarizeDocument",
        "timeSeriesAnalysis",
    }
)


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
    | TimeSeriesAnalysis,
    Field(discriminator="type"),
]


# --- Agent YAML resource ---


class SubagentConfig(BaseModelResource):
    agent_external_id: str = Field(
        description="External ID of the agent to use as a subagent.",
        min_length=1,
        max_length=255,
    )


class ExampleMessage(BaseModelResource):
    role: str = Field(description="The role of the expected message, e.g. 'function'.", min_length=1)
    content: str = Field(description="The content of the expected message.", min_length=1)


class ExampleQuestion(BaseModelResource):
    question: str = Field(description="An example question for the agent.", min_length=1)
    expected_messages: list[ExampleMessage] = Field(
        default_factory=list,
        description="Optional expected messages, such as tool-call hints.",
    )


Model = Literal[
    # Legacy model names without the azure/model naming convention
    "gpt-35-turbo",
    "gpt-35-turbo-16k",
    "gpt-4",
    "gpt-4-turbo",
    "gpt-4-32k",
    "gpt-4-vision",
    "gpt-4o-2024-05-13",
    "gpt-4o-2024-08-06",
    "bedrock/anthropic.claude-3-5-sonnet-20240620-v1:0",
    "bedrock/anthropic.claude-3-haiku-20240307-v1:0",
    "gpt-4o-mini",
    "gemini-1.5-pro",
    # Azure models
    "azure/o1",
    "azure/o3-mini",
    "azure/o3",
    "azure/o4-mini",
    "azure/gpt-3.5-turbo",
    "azure/gpt-3.5-turbo-16k",
    "azure/gpt-4",
    "azure/gpt-4-turbo",
    "azure/gpt-4-32k",
    "azure/gpt-4-vision",
    "azure/gpt-4o",
    "azure/gpt-4o-mini",
    "azure/gpt-4.1",
    "azure/gpt-4.1-nano",
    "azure/gpt-4.1-mini",
    "azure/gpt-5",
    "azure/gpt-5-mini",
    "azure/gpt-5-nano",
    "azure/gpt-5.1",
    "azure/gpt-5.2",
    "azure/gpt-5.4",
    "azure/gpt-5.4-mini",
    "azure/gpt-5.4-nano",
    "azure/gpt-5.5",
    # GCP models
    "gcp/claude-4-sonnet",
    "gcp/claude-4.5-sonnet",
    "gcp/claude-4.5-haiku",
    "gcp/claude-4-opus",
    "gcp/gemini-1.5-pro",
    "gcp/gemini-1.5-flash",
    "gcp/gemini-2.0-flash",
    "gcp/gemini-2.5-pro",
    "gcp/gemini-2.5-flash",
    "gcp/gemini-2.5-flash-lite",
    "gcp/gemini-3-pro-preview",
    "gcp/gemini-3.1-pro-preview",
    # AWS models
    "aws/claude-4.6-sonnet",
    "aws/claude-4.7-opus",
    "aws/claude-4.5-sonnet",
    "aws/claude-4.5-haiku",
    "aws/claude-4-sonnet",
    "aws/claude-4-opus",
    "aws/claude-4.1-opus",
    "aws/claude-3.5-sonnet",
    "aws/claude-3-haiku",
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
    subagents: list[SubagentConfig] | None = Field(
        None,
        description="List of agents to expose as subagents on this agent.",
        max_length=MAX_SUB_AGENTS_PER_AGENT,
    )
    skills: list[str] | None = Field(
        None,
        description="A list of skill external IDs available to the agent.",
        max_length=30,
    )
    labels: list[str] | None = Field(None, description="Labels for the agent, e.g. 'published'.")
    runtime_version: str | None = Field(None, description="The runtime version")
    example_questions: list[ExampleQuestion] | None = Field(
        None,
        description="Example questions shown to users to help them understand what the agent can do.",
        max_length=EXAMPLE_QUESTIONS_MAX_LENGTH,
    )

    @field_validator("subagents")
    @classmethod
    def validate_subagents_unique(cls, subagents: list[SubagentConfig] | None) -> list[SubagentConfig] | None:
        if not subagents:
            return subagents
        seen: set[str] = set()
        duplicates: list[str] = []
        for ref in subagents:
            if ref.agent_external_id in seen:
                duplicates.append(ref.agent_external_id)
            seen.add(ref.agent_external_id)
        if duplicates:
            raise ValueError(
                f"Duplicate subagent agentExternalId(s): {sorted(set(duplicates))}. "
                "Each entry must reference a distinct agent."
            )
        return subagents

    @model_validator(mode="after")
    def validate_subagents_configuration(self) -> Self:
        if not self.subagents:
            return self
        if any(ref.agent_external_id == self.external_id for ref in self.subagents):
            raise ValueError("An agent cannot reference itself as a subagent.")
        if self.runtime_version and self.runtime_version not in RUNTIME_VERSIONS_SUPPORTING_SUBAGENTS:
            raise ValueError(
                f"Runtime version '{self.runtime_version}' does not support subagents. "
                "Use a runtime version where supports_subagents is enabled, or remove the 'subagents' field."
            )
        if self.tools and any(tool.name == "delegate_to_subagent" for tool in self.tools):
            raise ValueError(
                "Tool name 'delegate_to_subagent' is reserved for the system sub-agent delegate tool. Rename the tool."
            )
        return self

    @model_validator(mode="after")
    def validate_example_questions_serialized_size(self) -> Self:
        if not self.example_questions:
            return self
        payload = {
            "questions": [
                question.model_dump(by_alias=True, exclude_unset=True) for question in self.example_questions
            ]
        }
        serialized_size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        if serialized_size > EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE:
            raise ValueError(
                f"Serialized exampleQuestions size is {serialized_size} bytes, which exceeds the maximum of "
                f"{EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE} bytes."
            )
        return self

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
