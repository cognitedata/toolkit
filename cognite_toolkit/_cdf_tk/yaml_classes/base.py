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

    def syntax_warning(self, source_file: Path) -> "ModelSyntaxWarning | None":
        """Return a build-time syntax warning for this resource, or ``None``.

        Called by the build system after a successful ``model_validate``.
        Override in subclasses that can accept YAML with soft issues (e.g. unknown
        capability names in ``GroupYAML``) while still producing a useful warning.
        """
        return None


T_Resource = TypeVar("T_Resource", bound=ToolkitResource)
