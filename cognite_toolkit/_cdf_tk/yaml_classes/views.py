import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId as ViewReferenceId
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamExternalId
from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS,
    FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import ToolkitResource
from .view_field_definitions import ViewPropertyType, ViewReference

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning, Recommendation
    from cognite_toolkit._cdf_tk.resource_ios import ResourceType

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)


class ViewYAML(ToolkitResource):
    space: str = Field(
        description="Id of the space that the view belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the view.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="Version of the view.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="name for the view.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the view.",
        max_length=1024,
    )
    filter: dict[str, Any] | None = Field(
        default=None,
        description="A filter Domain Specific Language (DSL) used to create advanced filter queries.",
    )
    implements: list[ViewReference] | None = Field(
        default=None,
        description="References to the views from where this view will inherit properties.",
    )
    stream_id: list[StreamExternalId] | None = Field(
        default=None,
        description=(
            "External ids of the records streams this view targets. "
            "Must be a single-element array in v1; multi-stream record views are reserved for future use."
        ),
        min_length=1,
        max_length=1,
    )
    properties: dict[str, ViewPropertyType] | None = Field(
        default=None, description="Set of properties to apply to the View."
    )

    def as_id(self) -> ViewReferenceId:
        return ViewReferenceId(space=self.space, external_id=self.external_id, version=self.version)

    def extra_insights(self, source_file: Path, resource_type: "ResourceType") -> "list[BuildWarning | Recommendation]":
        return [
            *super().extra_insights(source_file, resource_type),
            *self._filter_insights(source_file),
        ]

    def _filter_insights(self, source_file: Path) -> "list[BuildWarning | Recommendation]":
        """Reports view filters with nested filters, or with hasData filters on views."""
        if not Flags.ALPHA_RULES.is_enabled():
            return []
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning, Recommendation
        from cognite_toolkit._cdf_tk.rules._base import with_position

        view_id = self.as_id()
        expressions = list(_filter_expressions(self.filter))
        insights: list[BuildWarning | Recommendation] = []
        if any(filter_type == "nested" for filter_type, _ in expressions):
            recommendation = Recommendation(
                code="PERFORMANCE-DEGRADED",
                message=(
                    f"View '{view_id}' has a nested filter. Nested filters follow a direct relation for every "
                    "instance queried through the view, and are not supported by the instances inspect endpoint."
                ),
                fix=(
                    "Replace the nested filter with a filter on the properties of the view's own containers, "
                    "for example, a hasData filter on containers or an equals filter on the node type."
                ),
                source_file=source_file,
                alpha=True,
            )
            insights.append(with_position(recommendation, keys=["nested", "filter"]))
        view_labels = [
            label
            for filter_type, body in expressions
            if filter_type == "hasData"
            for label in _has_data_view_labels(body)
        ]
        if view_labels:
            noun = "view" if len(view_labels) == 1 else "views"
            warning = BuildWarning(
                code="MODELING-SUBOPTIMAL",
                message=(
                    f"View '{view_id}' has a hasData filter on the {noun} {humanize_collection(view_labels, sort=False)}. "
                    "A hasData filter on a view is resolved through the containers of that view, and CDF rejects "
                    f"it if that view refers back to '{view_id}'."
                ),
                fix="Use hasData on the containers of the referenced view, instead of the view itself.",
                source_file=source_file,
                alpha=True,
            )
            insights.append(with_position(warning, keys=["hasData", "filter"]))
        return insights

    @field_validator("external_id")
    @classmethod
    def check_forbidden_external_id_value(cls, val: str) -> str:
        """Check the external_id not present in forbidden set"""
        if val in FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS:
            raise ValueError(
                f"'{val}' is a reserved view External ID. Reserved External IDs are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS)}"
            )
        return val

    @field_validator("properties")
    @classmethod
    def validate_properties_identifier(cls, val: dict[str, str] | None) -> dict[str, str] | None:
        """Validate properties Identifier"""
        if val is None:
            return val
        for key in val:
            if not KEY_PATTERN.match(key):
                raise ValueError(f"Property '{key}' does not match the required pattern: {KEY_PATTERN.pattern}")
            if key in FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER:
                raise ValueError(
                    f"'{key}' is a reserved property identifier. Reserved identifiers are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER)}"
                )
        return val


def _filter_expressions(filter_: Any) -> Iterable[tuple[str, Any]]:
    """The filter type and body of each filter, including those within 'and', 'or', 'not', and 'nested'."""
    if not isinstance(filter_, dict):
        return
    for filter_type, body in filter_.items():
        yield filter_type, body
        if filter_type in ("and", "or") and isinstance(body, list):
            for item in body:
                yield from _filter_expressions(item)
        elif filter_type == "not":
            yield from _filter_expressions(body)
        elif filter_type == "nested" and isinstance(body, dict):
            yield from _filter_expressions(body.get("filter"))


def _has_data_view_labels(body: Any) -> list[str]:
    if not isinstance(body, list):
        return []
    return [
        f"'{ref.get('space')}:{ref.get('externalId')}(version={ref.get('version')})'"
        for ref in body
        if isinstance(ref, dict) and ref.get("type") == "view"
    ]
