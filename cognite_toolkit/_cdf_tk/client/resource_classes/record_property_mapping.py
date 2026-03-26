from typing import Literal, Self

from pydantic import ValidationError, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content


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


def load_record_migration_config_yaml(yaml_content: str) -> RecordMigrationConfig:
    """Parse YAML into `RecordMigrationConfig` for events-to-records."""
    content = read_yaml_content(yaml_content)
    if not isinstance(content, dict):
        raise ToolkitValueError(
            f"Expected a YAML mapping with streamExternalId, resourceType, and mappings; got {type(content).__name__}."
        )
    if "mappings" not in content:
        raise ToolkitValueError(
            "Missing required key 'mappings'. Top-level keys must include streamExternalId, resourceType, and mappings."
        )
    try:
        config = RecordMigrationConfig._load(content)
    except ValidationError as exc:
        raise ToolkitValueError(f"Invalid record migration config: {exc}") from exc
    if not config.mappings:
        raise ToolkitValueError("mappings must contain at least one record property mapping.")
    return config
