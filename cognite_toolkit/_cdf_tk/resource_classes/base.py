from dataclasses import dataclass
from typing import ClassVar

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel


class BaseModelResource(BaseModel, alias_generator=to_camel_case, extra="forbid"): ...


class ToolkitResource(BaseModelResource):
    _cdf_resource: ClassVar[type[CogniteResource]]

    def as_cognite_sdk_resource(self) -> CogniteResource:
        """Convert the model to a CDF resource."""
        return self._cdf_resource.load(self.model_dump(exclude_unset=True))


@dataclass(frozen=True)
class UnusedFieldWarning(Warning):
    """Warning for unused fields in the model."""

    field_names: frozenset[str]
