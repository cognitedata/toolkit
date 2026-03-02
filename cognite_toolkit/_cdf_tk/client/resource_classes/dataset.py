from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class DataSet(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    write_protected: bool | None = None

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert DataSet to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class DataSetRequest(DataSet, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"write_protected"})


class DataSetResponse(DataSet, ResponseResource[DataSetRequest]):
    id: int
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DataSetRequest]:
        return DataSetRequest
