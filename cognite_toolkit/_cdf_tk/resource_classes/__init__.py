"""Toolkit resource classes are pydantic models that represent the YAML file format the Cognite Toolkit uses.

This is means that we have three set of resource classes we use in Toolkit:

1. Toolkit resource classes (this module): Represent the YAML configuration format the Toolkit uses.
2. Write resource classes (from cognite-sdk): Represent the write/request format the Cognite resources.
3. Read resource classes (from cognite-sdk): Represent the read/response format the Cognite resources.
"""

from typing import ClassVar

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel


class BaseModelResource(BaseModel, alias_generator=to_camel_case, extra="ignore"):
    """Base class for all Toolkit resources class and the nested classes."""

    pass


class ToolkitResource(BaseModelResource):
    """The base class for all Toolkit resources, i.e., resource that has a YAML file format."""

    _cdf_resource: ClassVar[type[CogniteResource]]

    def as_cognite_sdk_resource(self) -> CogniteResource:
        """Convert the model to a CDF resource."""
        return self._cdf_resource.load(self.model_dump(exclude_unset=True))
