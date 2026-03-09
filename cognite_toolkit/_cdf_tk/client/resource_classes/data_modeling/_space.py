from abc import ABC

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import SpaceId


class Space(BaseModelObject, ABC):
    space: str
    name: str | None = None
    description: str | None = None

    def as_id(self) -> SpaceId:
        return SpaceId(space=self.space)


class SpaceRequest(Space, RequestResource): ...


class SpaceResponse(Space, ResponseResource[SpaceRequest]):
    created_time: int
    last_updated_time: int
    is_global: bool

    @classmethod
    def request_cls(cls) -> type[SpaceRequest]:
        return SpaceRequest
