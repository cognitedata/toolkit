from cognite_toolkit/_cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class RawTable(BaseModelObject):
    db_name: str
    table_name: str


class Contact(BaseModelObject):
    name: str | None = None
    email: str | None = None
    role: str | None = None
    send_notification: bool | None = None


class ExtractionPipeline(BaseModelObject):
    external_id: str
    name: str
    description: str | None = None
    data_set_id: int | None = None
    raw_tables: list[RawTable] | None = None
    schedule: str | None = None
    contacts: list[Contact] | None = None
    metadata: dict[str, str] | None = None
    source: str | None = None
    documentation: str | None = None
    created_by: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExtractionPipelineRequest(ExtractionPipeline, RequestResource):
    pass


class ExtractionPipelineResponse(ExtractionPipeline, ResponseResource[ExtractionPipelineRequest]):
    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> ExtractionPipelineRequest:
        return ExtractionPipelineRequest.model_validate(self.dump(), extra="ignore")
