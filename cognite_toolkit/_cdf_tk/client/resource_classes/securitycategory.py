from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import NameId


class SecurityCategory(BaseModelObject):
    name: str

    def as_id(self) -> NameId:
        return NameId(name=self.name)


class SecurityCategoryRequest(SecurityCategory, RequestResource): ...


class SecurityCategoryResponse(SecurityCategory, ResponseResource[SecurityCategoryRequest]):
    id: int

    def as_request_resource(self) -> SecurityCategoryRequest:
        return SecurityCategoryRequest.model_validate(self.dump(), extra="ignore")
