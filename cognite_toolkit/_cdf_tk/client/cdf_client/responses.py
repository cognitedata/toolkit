from typing import Generic, TypeVar

from pydantic import BaseModel, Field

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
