from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import InternalId


class TransformationNotification(BaseModelObject):
    destination: str


class TransformationNotificationRequest(TransformationNotification, RequestResource):
    transformation_external_id: str | None = None
    transformation_id: int | None = None
    # The id is not part of the create request payload, but is needed for deletion.
    id: int | None = Field(default=None, exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot create identifier from TransformationNotificationRequest without id set")
        return InternalId(id=self.id)


class TransformationNotificationResponse(
    TransformationNotification, ResponseResource[TransformationNotificationRequest]
):
    id: int
    created_time: int
    last_updated_time: int
    transformation_id: int
    transformation_external_id: str | None = None

    @classmethod
    def request_cls(cls) -> type[TransformationNotificationRequest]:
        return TransformationNotificationRequest
