import re
from collections.abc import Iterable

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning, Insight
from cognite_toolkit._cdf_tk.resource_ios import TransformationIO, ViewIO
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException, RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.yaml_classes.transformation_destination import DataModelSource, NodeViewDataSource
from cognite_toolkit._cdf_tk.yaml_classes.views import ViewYAML

# Inherited properties of the CDM views that data models most often implement.
_CDM_PROPERTIES: dict[str, set[str]] = {
    "CogniteDescribable": {"name", "description", "tags", "aliases"},
    "CogniteSourceable": {
        "sourceId",
        "sourceContext",
        "source",
        "sourceCreatedTime",
        "sourceUpdatedTime",
        "sourceCreatedUser",
        "sourceUpdatedUser",
    },
}
_NODE_FIELDS = {"externalId", "space"}


def select_aliases(sql: str) -> list[str]:
    """Output column names between the first SELECT and the first FROM."""
    sql = re.sub(r"--[^\n]*", "", sql)
    match = re.search(r"\bselect\b(.*?)\bfrom\b", sql, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return []
    items: list[str] = []
    depth = 0
    current = ""
    for char in match.group(1):
        if char == "," and depth == 0:
            items.append(current)
            current = ""
            continue
        depth += {"(": 1, ")": -1}.get(char, 0)
        current += char
    items.append(current)

    aliases: list[str] = []
    for item in items:
        item = item.strip()
        if alias_match := re.search(r"\bas\s+`?(\w+)`?\s*$", item, flags=re.IGNORECASE):
            aliases.append(alias_match.group(1))
        elif column_match := re.fullmatch(r"(?:`?\w+`?\.)?`?(\w+)`?", item):
            aliases.append(column_match.group(1))
    return aliases


def _view_properties(view_id: ViewId, views: dict[ViewId, ViewYAML]) -> set[str] | None:
    """Own and inherited property names, or None when a parent view is unknown."""
    view = views.get(view_id)
    if view is None:
        return None
    names = set(view.properties or {})
    for parent in view.implements or []:
        parent_id = parent.as_id()
        if parent_id in views:
            inherited = _view_properties(parent_id, views)
        elif parent_id.space == "cdf_cdm" and parent_id.external_id in _CDM_PROPERTIES:
            inherited = _CDM_PROPERTIES[parent_id.external_id]
        else:
            return None
        if inherited is None:
            return None
        names |= inherited
    return names


class TransformationRuleSet(ToolkitGlobalRuleSet):
    """Checks that transformations only select columns their destination view has a property for."""

    CODE_PREFIX = "TRANSFORMATION"
    DISPLAY_NAME = "Transformations"

    def get_status(self) -> RuleSetStatus:
        return RuleSetStatus(
            code="ready",
            message="Will validate the selected columns of transformations against destination views in the provided modules.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        views = self._local_views()
        transformation_type = TransformationIO.as_crud_type()
        for module in self.modules:
            for resource in module.resources:
                if resource.type == transformation_type and resource.can_verify:
                    yield from self._validate_transformation(resource, views)

    def _local_views(self) -> dict[ViewId, ViewYAML]:
        view_type = ViewIO.as_crud_type()
        views: dict[ViewId, ViewYAML] = {}
        for module in self.modules:
            for resource in module.resources:
                if resource.type != view_type or not resource.can_verify:
                    continue
                try:
                    view = ViewIO.validate_object(resource.read_built(), extra="ignore")
                except ValidationError:
                    continue
                views[view.as_id()] = view
        return views

    @classmethod
    def _validate_transformation(
        cls, resource: BuiltResource, views: dict[ViewId, ViewYAML]
    ) -> Iterable[Insight | InternalValidatorException]:
        try:
            transformation = TransformationIO.validate_object(resource.read_built(), extra="ignore")
        except ValidationError:
            return
        destination = transformation.destination
        view_id: ViewId | None = None
        if isinstance(destination, NodeViewDataSource) and destination.view:
            view_id = ViewId(
                space=destination.view.space,
                external_id=destination.view.external_id,
                version=destination.view.version,
            )
        elif isinstance(destination, DataModelSource) and not destination.data_model.destination_relationship_from_type:
            view_id = ViewId(
                space=destination.data_model.space,
                external_id=destination.data_model.destination_type,
                version=destination.data_model.version,
            )
        if view_id is None or not transformation.query or (known := _view_properties(view_id, views)) is None:
            return

        # The query is read from its own file when there is one, so the warning points there.
        source_file = next(
            (extra.source_path for extra in resource.extra_files if extra.resource_field == "query"),
            resource.source_path,
        )
        for alias in select_aliases(transformation.query):
            if alias in known or alias in _NODE_FIELDS:
                continue
            yield BuildWarning(
                code="MISSING-REFERENCED-PROPERTY",
                message=(
                    f"Transformation '{transformation.external_id}' selects '{alias}', "
                    f"which is not a property of its destination view {view_id}."
                ),
                fix=f"Add '{alias}' to the view, or remove it from the select list.",
                source_file=source_file,
            )
