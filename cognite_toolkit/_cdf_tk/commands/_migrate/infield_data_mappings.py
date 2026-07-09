from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter
from pydantic.config import ExtraValues

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    QueryEdgeExpression,
    QueryEdgeTableExpression,
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QuerySelect,
    QuerySelectSource,
    QuerySortSpec,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import EdgeOtherSide
from cognite_toolkit._cdf_tk.constants import SUBSELECTION_LIMIT_QUERY_ENDPOINT
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceQuerySelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, safe_read

DIRECT_RELATION_EDGE_TIEBREAKERS: dict[str, Callable[[list[EdgeOtherSide]], list[EdgeOtherSide]]] = {
    # This was observed as a potential artifact from previous Infield migrations.
    "referenceChecklistItems": lambda edges: (
        [edge for edge in edges if edge.edge_id.external_id.endswith("_relation")][:1] or edges
    ),
}


def create_infield_data_mappings(extra: ExtraValues | None = None) -> list[ViewToViewMapping]:
    mappings_path = Path(__file__).parent / "infield_data_mappings.yaml"

    content = safe_read(mappings_path)
    mappings_dict = read_yaml_content(content)
    if not isinstance(mappings_dict, list):
        raise ValueError(f"Expected a list of mappings in the YAML file, but got {type(mappings_dict).__name__}")
    return TypeAdapter(list[ViewToViewMapping]).validate_python(mappings_dict, extra=extra)


def resolve_observation_view_id(
    configs: Sequence[InFieldCDMLocationConfigResponse], target_space: str
) -> ViewId | None:
    """Determine which custom observation view, if any, Observations should be migrated to for a given
    target instance space, based on the ``viewMappings.observation`` entries of the InField CDM location
    configs already deployed for that space.

    Args:
        configs: All deployed InField CDM location configs (``client.infield.cdm_config.list()``).
        target_space: The instance space Infield data is being migrated to.

    Returns:
        The custom observation ``ViewId`` to migrate Observations to, or ``None`` if no location targeting
        ``target_space`` has a custom observation view configured (in which case the default
        ``cdf_infield/FieldObservation`` view should be used).

    Raises:
        ToolkitMigrationError: If locations targeting ``target_space`` specify conflicting observation views.

    """
    view_id_by_location: dict[str, ViewId] = {}
    for config in configs:
        if config.data_storage is None or config.data_storage.app_instance_space != target_space:
            continue
        observations = config.view_mappings.get("observation") if config.view_mappings else None
        if not isinstance(observations, list) or not observations:
            continue
        view = observations[0]
        if not isinstance(view, dict):
            continue
        view_id_by_location[config.external_id] = ViewId(
            space=str(view.get("space")), external_id=str(view.get("externalId")), version=str(view.get("version"))
        )

    distinct_view_ids = set(view_id_by_location.values())
    if not distinct_view_ids:
        return None
    if len(distinct_view_ids) == 1:
        return next(iter(distinct_view_ids))

    conflicts = ", ".join(f"{location}={view_id!s}" for location, view_id in view_id_by_location.items())
    raise ToolkitMigrationError(
        f"You have configured multiple InField locations targeting the same appInstanceSpace {target_space!r} with different observation "
        f"views: {conflicts}. Therefore, Toolkit cannot automatically determine which view to migrate Observations to. "
        "You need to use the --skip-observations flag and migrate Observations manually, or ensure all locations targeting this "
        f"appInstanceSpace share the same observation view. Distinct views found: "
        f"{humanize_collection([str(view_id) for view_id in distinct_view_ids])}."
    )


def create_infield_schedule_selector(instance_space: str | None = None) -> InstanceQuerySelector:
    """The migration of schedules is a bit more complex as we need to fetch all schedules connected to
    a single Template item along with all edges connecting them.

    The reason is that in the migration of schedules, instead of connecting:
    Template -> TemplateItem -> Schedule, we are going to connect Schedule -> TemplateItem and Schedule -> Template.
    In addition, to remove duplicated schedules (of which there are many).

    Args:
        instance_space: If provided, only selects schedules connected to template items in the specified space.
            Note that the schedule, template items and edges will be selected regardless of their space.

    Returns:
        An InstanceQuerySelector that selects all schedules connected to a single template item along with the edges connecting them.

    """
    template = ViewId(space="cdf_apm", external_id="Template", version="v8")
    item = ViewId(space="cdf_apm", external_id="TemplateItem", version="v7")
    schedule = ViewId(space="cdf_apm", external_id="Schedule", version="v4")
    template_filter: dict[str, Any] = {"hasData": [template.dump(include_type=True)]}
    if instance_space is not None:
        template_filter = {
            "and": [template_filter, {"equals": {"property": ["node", "space"], "value": instance_space}}]
        }
    return InstanceQuerySelector(
        endpoint="query",
        query=QueryRequest(
            with_={
                "template": QueryNodeExpression(
                    limit=1,
                    nodes=QueryNodeTableExpression(filter=template_filter),
                    sort=[QuerySortSpec(property=["node", "space"]), QuerySortSpec(property=["node", "externalId"])],
                ),
                "templateEdges": QueryEdgeExpression(
                    limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                    edges=QueryEdgeTableExpression(
                        from_="template",
                        chain_to="source",
                        direction="outwards",
                        filter={
                            "equals": {
                                "property": ["edge", "type"],
                                "value": {"space": "cdf_apm", "externalId": "referenceTemplateItems"},
                            }
                        },
                    ),
                ),
                "templateItem": QueryNodeExpression(
                    limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                    nodes=QueryNodeTableExpression(
                        from_="templateEdges",
                        chain_to="destination",
                        filter={"hasData": [item.dump(include_type=True)]},
                    ),
                ),
                "templateItemEdges": QueryEdgeExpression(
                    limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                    edges=QueryEdgeTableExpression(
                        from_="templateItem",
                        chain_to="source",
                        direction="outwards",
                        filter={
                            "equals": {
                                "property": ["edge", "type"],
                                "value": {"space": "cdf_apm", "externalId": "referenceSchedules"},
                            }
                        },
                    ),
                ),
                "schedules": QueryNodeExpression(
                    limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                    nodes=QueryNodeTableExpression(
                        from_="templateItemEdges",
                        chain_to="destination",
                        filter={"hasData": [schedule.dump(include_type=True)]},
                    ),
                ),
            },
            select={
                "schedules": QuerySelect(sources=[QuerySelectSource(source=schedule, properties=["*"])]),
                "template": QuerySelect(sources=[QuerySelectSource(source=template, properties=["*"])]),
                "templateItemEdges": QuerySelect(),
                "templateEdges": QuerySelect(),
            },
            root="template",
        ).model_dump_json(),
        root="template",
        subselections=tuple(["schedules", "templateItemEdges", "templateEdges"]),
    )
