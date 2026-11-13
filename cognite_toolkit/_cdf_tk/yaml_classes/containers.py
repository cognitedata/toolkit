import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ContainerId
from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS,
    FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import ToolkitResource
from .container_field_definitions import (
    BooleanPrimitiveProperty,
    BtreeIndex,
    ConstraintType,
    ContainerPropertyDefinition,
    DatePrimitiveProperty,
    Float32PrimitiveProperty,
    Float64PrimitiveProperty,
    IndexType,
    Int32PrimitiveProperty,
    Int64PrimitiveProperty,
    ListablePropertyTypeDefinition,
    PropertyTypeDefinition,
    TextProperty,
    TimestampPrimitiveProperty,
)

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning, Recommendation
    from cognite_toolkit._cdf_tk.resource_ios import ResourceType

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)

_DEPRECATED_CDF_REFERENCES = {
    "timeseries": "asset-centric time series in the Time series API",
    "file": "asset-centric files in the Files API",
    "sequence": "Sequences API",
}
_DEPRECATED_CDF_REFERENCE_FIXES = {
    "timeseries": "Use a direct relation to a CogniteTimeSeries instance instead.",
    "file": "Use a direct relation to a CogniteFile instance instead.",
    "sequence": (
        "Use a direct relation to a CogniteFile instance for tables downloaded as a whole, "
        "or records for multi-column data that needs row-level queries."
    ),
}
# CDF limits the size of a B-tree index entry, which is why int32 and int64 lists can have at most 600 and 300
# elements in a B-tree index.
_BTREE_MAX_BYTES = 2400
_BTREE_LIST_ELEMENT_BYTES: dict[type[PropertyTypeDefinition], int] = {
    BooleanPrimitiveProperty: 1,
    DatePrimitiveProperty: 8,
    Float32PrimitiveProperty: 4,
    Float64PrimitiveProperty: 8,
    Int32PrimitiveProperty: 4,
    Int64PrimitiveProperty: 8,
    TimestampPrimitiveProperty: 8,
}


class ContainerYAML(ToolkitResource):
    space: str = Field(
        description="The workspace for the container, a unique identifier for the space.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the container.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="name for the container.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the container.",
        max_length=1024,
    )
    used_for: Literal["node", "edge", "record", "all"] | None = Field(
        default=None,
        description="Should this operation apply to nodes, edges or both.",
    )
    properties: dict[str, ContainerPropertyDefinition] = Field(
        description="Set of properties to apply to the container."
    )
    constraints: dict[str, ConstraintType] | None = Field(
        default=None,
        description="Set of constraints to apply to the container.",
    )
    indexes: dict[str, IndexType] | None = Field(
        default=None,
        description="Set of indexes to apply to the container.",
        max_length=10,
    )

    def as_id(self) -> ContainerId:
        return ContainerId(space=self.space, external_id=self.external_id)

    def extra_insights(self, source_file: Path, resource_type: "ResourceType") -> "list[BuildWarning | Recommendation]":
        return [
            *super().extra_insights(source_file, resource_type),
            *self._deprecated_cdf_reference_warnings(source_file),
            *self._btree_index_size_insights(source_file),
        ]

    def _deprecated_cdf_reference_warnings(self, source_file: Path) -> "list[BuildWarning]":
        """Warn against properties referencing time series, files, or sequences by their classic external ID."""
        if not Flags.ALPHA_RULES.is_enabled():
            return []
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning
        from cognite_toolkit._cdf_tk.rules._base import with_position

        return [
            with_position(
                BuildWarning(
                    code="DEPRECATED-FEATURE",
                    message=(
                        f"Property '{prop_id}' in container {self.space}:{self.external_id} is of type "
                        f"'{prop.type.type}', which references the deprecated {_DEPRECATED_CDF_REFERENCES[prop.type.type]}. "
                        "The API will be retired on December 31, 2027."
                    ),
                    source_file=source_file,
                    fix=_DEPRECATED_CDF_REFERENCE_FIXES[prop.type.type],
                    alpha=True,
                ),
                keys=[prop_id],
            )
            for prop_id, prop in self.properties.items()
            if prop.type.type in _DEPRECATED_CDF_REFERENCES
        ]

    def _btree_index_size_insights(self, source_file: Path) -> "list[BuildWarning | Recommendation]":
        """Reports B-tree indexed properties without maxListSize or maxTextSize, or with size bounds above the limit.

        CDF requires maxListSize on list properties in new B-tree indexes. Values larger than a B-tree index entry
        make ingestion fail, and pre-existing ones make the index fail to build.
        """
        if not Flags.ALPHA_RULES.is_enabled():
            return []
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning, Recommendation
        from cognite_toolkit._cdf_tk.rules._base import with_position

        container_id = f"{self.space}:{self.external_id}"
        insights: list[BuildWarning | Recommendation] = []
        for index_id, index in (self.indexes or {}).items():
            if not isinstance(index, BtreeIndex):
                continue
            type_by_property = {
                prop_id: self.properties[prop_id].type for prop_id in index.properties if prop_id in self.properties
            }
            for prop_id, prop_type in type_by_property.items():
                if _is_missing_max_list_size(prop_type):
                    warning = BuildWarning(
                        code="MISSING-VALUE",
                        message=(
                            f"List property '{prop_id}' in B-tree index '{index_id}' of container '{container_id}' "
                            "has no maxListSize. CDF requires maxListSize on list properties in a new B-tree index, "
                            "so deploying the index fails."
                        ),
                        fix=(
                            f"Set maxListSize on property '{prop_id}', such that its values are at most "
                            f"{_BTREE_MAX_BYTES} bytes."
                        ),
                        source_file=source_file,
                        alpha=True,
                    )
                    insights.append(with_position(warning, keys=[prop_id, index_id]))
                if _is_missing_max_text_size(prop_type):
                    recommendation = BuildWarning(
                        code="MISSING-VALUE",
                        message=(
                            f"Text property '{prop_id}' in B-tree index '{index_id}' of container '{container_id}' "
                            f"has no maxTextSize. Values larger than {_BTREE_MAX_BYTES} bytes make ingestion fail, "
                            "or the index fail to build."
                        ),
                        fix=(
                            f"Set maxTextSize on property '{prop_id}', such that its values are at most "
                            f"{_BTREE_MAX_BYTES} bytes."
                        ),
                        source_file=source_file,
                        alpha=True,
                    )
                    insights.append(with_position(recommendation, keys=[prop_id, index_id]))
            size_by_property = {
                prop_id: size
                for prop_id, prop_type in type_by_property.items()
                if (size := _btree_size_bound(prop_type))
            }
            if sum(size_by_property.values()) <= _BTREE_MAX_BYTES:
                continue
            properties = humanize_collection([f"'{prop_id}'" for prop_id in size_by_property], sort=False)
            recommendation = BuildWarning(
                code="EXCEEDED-LIMIT",
                message=(
                    f"B-tree index '{index_id}' of container '{container_id}' allows values of up to "
                    f"{sum(size_by_property.values())} bytes, which exceeds the limit of {_BTREE_MAX_BYTES} bytes. "
                    "Larger values make ingestion fail, or the index fail to build."
                ),
                fix=(
                    f"Lower maxListSize or maxTextSize on {properties} to at most {_BTREE_MAX_BYTES} bytes combined. "
                    "A text value counts maxTextSize bytes, an int32 list element 4 bytes, and an int64 list "
                    "element 8 bytes."
                ),
                source_file=source_file,
                alpha=True,
            )
            insights.append(with_position(recommendation, keys=[index_id]))
        return insights

    @field_validator("external_id")
    @classmethod
    def check_forbidden_external_id_value(cls, val: str) -> str:
        """Check the external_id not present in forbidden set"""
        if val in FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS:
            raise ValueError(
                f"'{val}' is a reserved container External ID. Reserved External IDs are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS)}"
            )
        return val

    @field_validator("properties")
    @classmethod
    def validate_properties_identifier(cls, val: dict[str, str]) -> dict[str, str]:
        """Validate properties Identifier"""
        for key in val.keys():
            if not KEY_PATTERN.match(key):
                raise ValueError(f"Property '{key}' does not match the required pattern: {KEY_PATTERN.pattern}")
            if key in FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER:
                raise ValueError(
                    f"'{key}' is a reserved property identifier. Reserved identifiers are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER)}"
                )
        return val

    @field_validator("indexes", "constraints", mode="after")
    @classmethod
    def indexes_and_constraints_not_supported_for_record_containers(
        cls, val: dict[str, str] | None, info: ValidationInfo
    ) -> dict[str, str] | None:
        """Validate that indexes and constraints are not set for record containers"""
        if info.data.get("used_for") == "record" and val is not None:
            raise ValueError("Indexes and constraints are not supported for record containers.")
        return val

    @field_validator("properties", mode="after")
    @classmethod
    def external_cdf_references_not_supported_for_record_containers(
        cls, val: dict[str, ContainerPropertyDefinition], info: ValidationInfo
    ) -> dict[str, ContainerPropertyDefinition]:
        """Validate that external CDF references are not set for record containers"""
        if info.data.get("used_for") == "record":
            for key, prop in val.items():
                if prop.type.type in {"timeseries", "file", "sequence"}:
                    raise ValueError(
                        f"External CDF references are not supported for record containers. Property '{key}' is of type 'externalCdfReference'."
                    )
        return val


def _is_missing_max_list_size(prop_type: PropertyTypeDefinition) -> bool:
    return (
        isinstance(prop_type, ListablePropertyTypeDefinition)
        and bool(prop_type.list)
        and prop_type.max_list_size is None
    )


def _is_missing_max_text_size(prop_type: PropertyTypeDefinition) -> bool:
    return isinstance(prop_type, TextProperty) and prop_type.max_text_size is None


def _btree_size_bound(prop_type: PropertyTypeDefinition) -> int:
    """The size in bytes that maxListSize and maxTextSize bound an indexed value to, or 0 if they do not apply."""
    if not isinstance(prop_type, ListablePropertyTypeDefinition):
        return 0
    count = (prop_type.max_list_size or 0) if prop_type.list else 1
    if isinstance(prop_type, TextProperty):
        return count * (prop_type.max_text_size or 0)
    if prop_type.list:
        return count * _BTREE_LIST_ELEMENT_BYTES.get(type(prop_type), 0)
    return 0
