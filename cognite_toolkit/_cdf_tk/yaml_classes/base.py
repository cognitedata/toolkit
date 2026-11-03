from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning, Recommendation
    from cognite_toolkit._cdf_tk.resource_ios import ResourceType


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid", defer_build=True): ...


class ToolkitResource(BaseModelResource):
    @abstractmethod
    def as_id(self) -> Identifier:
        """Return an identifier for this resource."""
        raise NotImplementedError()

    def extra_insights(self, source_file: Path, resource_type: "ResourceType") -> "list[BuildWarning | Recommendation]":
        """Return build-time syntax warnings and recommendations after a successful ``model_validate``."""
        return self._missing_data_set_recommendations(source_file, resource_type)

    def _missing_data_set_recommendations(
        self, source_file: Path, resource_type: "ResourceType"
    ) -> "list[BuildWarning | Recommendation]":
        """Recommend scoping a resource to a data set when it supports it.

        Without a data set, access control cannot be properly enforced for the resource. Some resources
        (e.g. functions) can be scoped to a space instead, so a set space is also accepted.
        """
        fields = type(self).model_fields
        if "data_set_external_id" not in fields or getattr(self, "data_set_external_id", None) is not None:
            return []
        supports_space = "space" in fields
        if supports_space and getattr(self, "space", None) is not None:
            return []
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
        from cognite_toolkit._cdf_tk.rules._base import quote_identifier

        kind = resource_type.kind
        if supports_space:
            message = f"{kind} {quote_identifier(self.as_id())} is not assigned to a data set or space."
            fix = f"Set 'dataSetExternalId' or 'space' on the {kind}."
        else:
            message = f"{kind} {quote_identifier(self.as_id())} is not assigned to a data set."
            fix = f"Set 'dataSetExternalId' on the {kind}."
        return [Recommendation(code="UNGOVERNED-RESOURCE", message=message, fix=fix, source_file=source_file)]


T_Resource = TypeVar("T_Resource", bound=ToolkitResource)
