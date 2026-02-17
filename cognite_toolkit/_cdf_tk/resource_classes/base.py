from abc import abstractmethod

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid"): ...


class ToolkitResource(BaseModelResource):
    @abstractmethod
    def as_id(self) -> Identifier:
        """Return an identifier for this resource."""
        raise NotImplementedError()
