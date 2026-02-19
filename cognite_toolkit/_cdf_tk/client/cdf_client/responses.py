from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, JsonValue

T = TypeVar("T", bound=BaseModel)


class ResponseItems(BaseModel, Generic[T]):
    """A page of reference items from a paginated API response.

    Attributes:
        items: The list of reference items in this page.
    """

    items: list[T]


class PagedResponse(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = Field(None, alias="nextCursor")


class QueryResponse(BaseModel):
    items: dict[str, list[dict[str, Any]]]
    typing: dict[str, JsonValue] | None = None
    next_cursor: dict[str, str] = Field(alias="nextCursor")
    debug: dict[str, JsonValue] | None = None
