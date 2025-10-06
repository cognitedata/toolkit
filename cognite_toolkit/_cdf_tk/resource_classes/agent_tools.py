from typing import Annotated, Literal

from pydantic import Field

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


class AgentDataModel(BaseModelResource):
    space: str = Field(
        description="The space the data model is in.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="The space the data model is in.",
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
        "v1",
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


AgentTool = Annotated[
    AskDocument | QueryKnowledgeGraph | QueryTimeSeriesDatapoints | SummarizeDocument,
    Field(discriminator="type"),
]
