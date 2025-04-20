from typing import ClassVar

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, ValidationError


class BaseModelResource(BaseModel, alias_generator=to_camel_case, extra="ignore"):
    pass
    # Todo; Warning on unknown extra unknown fields.


class ToolkitResource(BaseModelResource):
    _cdf_resource: ClassVar[type[CogniteResource]]

    def as_cognite_sdk_resource(self) -> CogniteResource:
        """Convert the model to a CDF resource."""
        return self._cdf_resource.load(self.model_dump(exclude_unset=True))


def as_message(e: ValidationError, context: str) -> str:
    errors: list[str] = []
    for no, error in enumerate(e.errors(), 1):
        error_type = error.get("type")
        location = ".".join(map(str, error.get("loc", tuple())))
        source_msg = error.get("msg", "")
        if error_type == "missing":
            msg = f"Error {no} {source_msg}: {location!r}"
        else:
            msg = f"Error {no} in location {location!r}: {error.get('msg', '')}"
        errors.append(msg)

    errors_str = "\n  - ".join(errors)
    return f"Failed to {context}: {len(errors)} errors in validation\n  - {errors_str}"
