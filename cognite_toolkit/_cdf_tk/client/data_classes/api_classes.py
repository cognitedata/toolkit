from typing import Generic, TypeVar

from pydantic import BaseModel, Field, JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestResource

T = TypeVar("T", bound=BaseModel)


class PagedResponse(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = Field(None, alias="nextCursor")


class QueryResponse(BaseModel, Generic[T]):
    items: dict[str, list[T]]
    typing: dict[str, JsonValue] | None = None
    next_cursor: dict[str, str] = Field(alias="nextCursor")
    debug: dict[str, JsonValue] | None = None


class InternalIdRequest(RequestResource):
    id: int

    def as_id(self) -> int:
        return self.id

    @classmethod
    def from_ids(cls, ids: list[int]) -> list["InternalIdRequest"]:
        return [cls(id=id_) for id_ in ids]
