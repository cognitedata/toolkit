from collections import defaultdict
from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class SequenceColumnSlim(BaseModelObject):
    external_id: str
    name: str | None = None
    value_type: Literal["STRING", "DOUBLE", "LONG"] | None = None


class SequenceColumn(SequenceColumnSlim):
    description: str | None = None
    metadata: dict[str, str] | None = None


class SequenceColumnRequest(SequenceColumn, UpdatableRequestResource):
    def as_id(self) -> Identifier:
        return ExternalId(external_id=self.external_id)


class SequenceColumnResponse(SequenceColumn, ResponseResource[SequenceColumnRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SequenceColumnRequest]:
        return SequenceColumnRequest


class Sequence(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    description: str | None = None
    asset_id: int | None = None
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert Sequence to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class SequenceRequest(Sequence, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "columns"})
    columns: list[SequenceColumnRequest]

    def as_update(
        self, mode: Literal["patch", "replace"], last_columns: list[SequenceColumn] | None = None
    ) -> dict[str, Any]:
        output = super().as_update(mode)
        output["update"].pop("columns", None)
        exclude_unset = mode == "patch"
        existing_ids = {col.external_id for col in last_columns} if last_columns is not None else set()
        columns: dict[str, list[Any]] = defaultdict(list)
        for col in self.columns:
            col_dumped = col.model_dump(mode="json", by_alias=True, exclude_none=True)
            if col_dumped == {"externalId": col.external_id}:
                continue  # No changes to this column

            if col.external_id in existing_ids:
                columns["modify"].append(col.as_update(mode=mode))
            else:
                columns["add"].append(col.model_dump(mode="json", by_alias=True, exclude_unset=exclude_unset))
        if mode == "replace":
            current_columns_ids = {col.external_id for col in self.columns}
            for last_col in last_columns or []:
                if last_col.external_id not in current_columns_ids:
                    columns["remove"].append({"externalId": last_col.external_id})
        if columns:
            output["update"]["columns"] = dict(columns)
        return output


class SequenceResponse(Sequence, ResponseResource[SequenceRequest]):
    id: int
    created_time: int
    last_updated_time: int
    columns: list[SequenceColumnResponse]

    @classmethod
    def request_cls(cls) -> type[SequenceRequest]:
        return SequenceRequest
