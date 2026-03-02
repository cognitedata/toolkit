from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class Label(BaseModelObject):
    external_id: str
    name: str
    description: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class LabelRequest(Label, RequestResource): ...


class LabelResponse(Label, ResponseResource[LabelRequest]):
    created_time: int

    @classmethod
    def request_cls(cls) -> type[LabelRequest]:
        return LabelRequest
