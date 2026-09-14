from abc import abstractmethod
from collections.abc import Mapping
from typing import ClassVar, TypeVar

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid"): ...


class ToolkitResource(BaseModelResource):
    externalFileMapping: ClassVar[Mapping[str, str]] = {}

    @abstractmethod
    def as_id(self) -> Identifier:
        """Return an identifier for this resource."""
        raise NotImplementedError()


T_Resource = TypeVar("T_Resource", bound=ToolkitResource)
