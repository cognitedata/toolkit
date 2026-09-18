from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModelSyntaxWarning


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid"): ...


class ToolkitResource(BaseModelResource):
    @abstractmethod
    def as_id(self) -> Identifier:
        """Return an identifier for this resource."""
        raise NotImplementedError()

    def syntax_warnings(self, source_file: Path) -> "list[ModelSyntaxWarning]":
        """Return build-time syntax warnings after a successful ``model_validate``."""
        return []


T_Resource = TypeVar("T_Resource", bound=ToolkitResource)
