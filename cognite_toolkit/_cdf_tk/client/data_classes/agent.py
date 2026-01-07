from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestUpdateable, ResponseResource

from .identifiers import ExternalId


class AgentToolDefinition(BaseModelObject):
    type: str
    name: str
    description: str


class AskDocument(AgentToolDefinition):
    type: Literal["askDocument"] = "askDocument"


class AgentDataModel(BaseModelObject):
    space: str
    external_id: str
    version: str
    view_external_ids: list[str]


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
    instance_spaces: AgentInstanceSpaces
    version: Literal["v1", "v2"] = "v2"


class QueryKnowledgeGraph(AgentToolDefinition):
    type: Literal["queryKnowledgeGraph"] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphConfig


class QueryTimeSeriesDatapoints(AgentToolDefinition):
    type: Literal["queryTimeSeriesDatapoints"] = "queryTimeSeriesDatapoints"


class SummarizeDocument(AgentToolDefinition):
    type: Literal["summarizeDocument"] = "summarizeDocument"


AgentTool = Annotated[
    AskDocument | QueryKnowledgeGraph | QueryTimeSeriesDatapoints | SummarizeDocument,
    Field(discriminator="type"),
]


class AgentRequest(RequestUpdateable):
    container_fields = frozenset({"tools"})
    non_nullable_fields = frozenset[str]()
    external_id: str | None = None
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str = "azure/gpt-4o-mini"
    tools: list[AgentTool] | None = None
    runtime_version: str | None = None

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert AgentRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class AgentResponse(ResponseResource[AgentRequest]):
    created_time: int
    last_updated_time: int
    id: int
    external_id: str | None = None
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str = "azure/gpt-4o-mini"
    tools: list[AgentTool] | None = None
    runtime_version: str | None = None

    def as_request_resource(self) -> AgentRequest:
        return AgentRequest.model_validate(self.dump(), extra="ignore")

