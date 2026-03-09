import builtins
from typing import Any, ClassVar, Literal

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId


class SignalSink(BaseModelObject):
    type: Literal["email", "user"]
    external_id: str

    def as_id(self) -> SignalSinkId:
        return SignalSinkId(type=self.type, external_id=self.external_id)


class SignalSinkRequest(SignalSink, UpdatableRequestResource):
    email_address: str | None = None

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"email_address"})

    @model_validator(mode="after")
    def _email_address_required_for_email_type(self) -> "SignalSinkRequest":
        if self.type == "email" and not self.email_address:
            raise ValueError("emailAddress is required when type is 'email'")
        return self

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = super().as_update(mode)
        # type is immutable — remove from update payload if present
        update_item.get("update", {}).pop("type", None)
        return update_item


class SignalSinkResponse(SignalSink, ResponseResource[SignalSinkRequest]):
    email_address: str | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> builtins.type[SignalSinkRequest]:
        return SignalSinkRequest
