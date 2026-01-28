from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import InternalIdUnwrapped


class SecurityCategory(BaseModelObject):
    name: str


class SecurityCategoryRequest(SecurityCategory, RequestResource):
    # This is not part of the request payload.
    id: int | None = Field(None, exclude=True)

    def as_id(self) -> InternalIdUnwrapped:
        if self.id is None:
            raise ValueError("Cannot create identifier from SecurityCategoryRequest without id set")
        return InternalIdUnwrapped(id=self.id)


class SecurityCategoryResponse(SecurityCategory, ResponseResource[SecurityCategoryRequest]):
    id: int

    def as_request_resource(self) -> SecurityCategoryRequest:
        return SecurityCategoryRequest.model_validate(self.dump(), extra="ignore")
