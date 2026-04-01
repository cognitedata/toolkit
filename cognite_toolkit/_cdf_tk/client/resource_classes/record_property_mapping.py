import sys
from typing import Literal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId


class RecordPropertyMapping(BaseModelObject):
    """One target container + property map for records migration."""

    external_id: str
    container_id: ContainerId
    property_mapping: dict[str, str]


class RecordMigrationConfig(BaseModelObject):
    """Root YAML model for events-to-records: target stream, resource type, and named mappings."""

    stream_external_id: str
    resource_type: Literal["event"]
    mappings: list[RecordPropertyMapping]
    default_mapping: str | None = None

    @model_validator(mode="after")
    def _validate_mappings(self) -> Self:
        if not self.mappings:
            raise ValueError("mappings must contain at least one entry.")
        seen: set[str] = set()
        for mapping in self.mappings:
            if mapping.external_id in seen:
                raise ValueError(f"Duplicate externalId in record property mappings: {mapping.external_id!r}")
            seen.add(mapping.external_id)
        if self.default_mapping is not None and self.default_mapping not in seen:
            raise ValueError(
                f"defaultMapping {self.default_mapping!r} does not match any mapping externalId. "
                f"Available: {sorted(seen)}."
            )
        return self
