"""Group request and response classes.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import InternalId

from .capability import GroupCapability


class TokenAttributes(BaseModelObject):
    """Token attributes for group."""

    app_ids: list[str] | None = None


class GroupAttributes(BaseModelObject):
    """Attributes for a group."""

    token: TokenAttributes | None = None


class Group(BaseModelObject):
    """Base class for Group resources."""

    name: str
    capabilities: list[GroupCapability] | None = None
    metadata: Metadata | None = None
    attributes: GroupAttributes | None = None
    source_id: str | None = Field(None, coerce_numbers_to_str=True)
    members: list[str] | Literal["allUserAccounts"] | None = None


class GroupRequest(Group, RequestResource):
    """Group request resource for creating/updating groups."""

    id: int | None = Field(default=None, exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot convert GroupRequest to InternalId when id is None")
        return InternalId(id=self.id)


class GroupResponse(Group, ResponseResource[GroupRequest]):
    """Group response resource returned from API."""

    id: int
    is_deleted: bool
    deleted_time: int | None = None

    @classmethod
    def request_cls(cls) -> type[GroupRequest]:
        return GroupRequest
