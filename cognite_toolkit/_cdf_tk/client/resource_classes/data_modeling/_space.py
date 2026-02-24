from abc import ABC

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from ._references import SpaceReference


class Space(BaseModelObject, ABC):
    space: str
    name: str | None = None
    description: str | None = None

    def as_id(self) -> SpaceReference:
        return SpaceReference(space=self.space)


class SpaceRequest(Space, RequestResource): ...


class SpaceResponse(Space, ResponseResource[SpaceRequest]):
    created_time: int
    last_updated_time: int
    is_global: bool

    @classmethod
    def request_cls(cls) -> type[SpaceRequest]:
        return SpaceRequest
