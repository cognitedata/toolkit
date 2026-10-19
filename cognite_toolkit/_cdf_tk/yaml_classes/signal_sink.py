from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId

from .base import ToolkitResource


class SignalSink(ToolkitResource):
    type: Literal["email", "user"]
    external_id: str = Field(
        description="The external ID of the sink.",
        min_length=1,
        max_length=255,
    )

    def as_id(self) -> SignalSinkId:
        return SignalSinkId(type=self.type, external_id=self.external_id)


class EmailSinkYAML(SignalSink):
    type: Literal["email"] = Field("email")
    email_address: str = Field(
        description="The e-mail address to send signals to.",
        min_length=3,
        max_length=255,
    )


class UserSinkYAML(SignalSink):
    type: Literal["user"] = Field("user")


SignalSinkYAML = Annotated[EmailSinkYAML | UserSinkYAML, Field(discriminator="type")]
