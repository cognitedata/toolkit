import sys
from types import MappingProxyType
from typing import Any, ClassVar, cast

from pydantic import Field, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import ToolkitResource

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class SignalSinkYAML(ToolkitResource):
    type: ClassVar[str]
    external_id: str = Field(
        description="The external ID of the sink.",
        min_length=1,
        max_length=255,
    )

    @model_validator(mode="wrap")
    @classmethod
    def find_sink_type(cls, data: "dict[str, Any] | SignalSinkYAML", handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, SignalSinkYAML):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid signal sink data '{type(data)}' expected dict")

        if cls is not SignalSinkYAML:
            return handler(data)

        if "type" not in data:
            raise ValueError("Missing required field: 'type'")
        type_ = data["type"]
        if type_ not in _SINK_CLS_BY_TYPE:
            raise ValueError(
                f"Invalid signal sink type '{type_}'. "
                f"Expected one of {humanize_collection(_SINK_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _SINK_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    def as_id(self) -> SignalSinkId:
        return SignalSinkId(type=cast(Any, self.type), external_id=self.external_id)

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


class EmailSinkYAML(SignalSinkYAML):
    type: ClassVar[str] = "email"
    email_address: str = Field(
        description="The e-mail address to send signals to.",
        min_length=3,
        max_length=255,
    )


class UserSinkYAML(SignalSinkYAML):
    type: ClassVar[str] = "user"


_SINK_CLS_BY_TYPE: MappingProxyType[str, type[SignalSinkYAML]] = MappingProxyType(
    {"email": EmailSinkYAML, "user": UserSinkYAML}
)
