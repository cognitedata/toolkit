from typing import Literal

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId

from .base import ToolkitResource


class SignalSinkYAML(ToolkitResource):
    type: Literal["email", "user"] = Field(
        description="The type of sink: 'email' for sending signals to a specified address, "
        "'user' for sending to the user's registered e-mail.",
    )
    external_id: str = Field(
        description="The external ID of the sink.",
        min_length=1,
        max_length=255,
    )
    email_address: str | None = Field(
        default=None,
        description="The e-mail address to send signals to. Required for 'email' type sinks.",
        min_length=3,
        max_length=255,
    )

    @model_validator(mode="after")
    def _email_address_required_for_email_type(self) -> "SignalSinkYAML":
        if self.type == "email" and not self.email_address:
            raise ValueError("emailAddress is required when type is 'email'")
        return self

    def as_id(self) -> SignalSinkId:
        return SignalSinkId(type=self.type, external_id=self.external_id)
