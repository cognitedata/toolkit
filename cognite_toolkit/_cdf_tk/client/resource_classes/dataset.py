from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class DataSet(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    write_protected: bool = False

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert DataSet to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class DataSetRequest(DataSet, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})


class DataSetResponse(DataSet, ResponseResource[DataSetRequest]):
    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DataSetRequest:
        return DataSetRequest.model_validate(self.dump(), extra="ignore")
