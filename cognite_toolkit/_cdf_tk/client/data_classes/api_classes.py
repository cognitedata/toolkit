from typing import Generic, TypeVar

from pydantic import BaseModel, Field, JsonValue

T = TypeVar("T", bound=BaseModel)


class PagedResponse(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = Field(None, alias="nextCursor")


class QueryResponse(BaseModel, Generic[T]):
    items: dict[str, list[T]]
    typing: dict[str, JsonValue] | None = None
    next_cursor: dict[str, str] = Field(alias="nextCursor")
    debug: dict[str, JsonValue] | None = None
