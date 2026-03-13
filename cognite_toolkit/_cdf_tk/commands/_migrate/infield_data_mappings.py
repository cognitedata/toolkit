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
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.constants import SUBSELECTION_LIMIT_QUERY_ENDPOINT
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceQuerySelector
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file


def create_infield_data_mappings(extra: ExtraValues | None = None) -> list[ViewToViewMapping]:
    mappings_path = Path(__file__).parent / "infield_data_mappings.yaml"
    mappings_dict = read_yaml_file(mappings_path, expected_output="list")
    return TypeAdapter(list[ViewToViewMapping]).validate_python(mappings_dict, extra=extra)


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
        ).model_dump_json(),
        root="template",
        subselections=tuple(["schedules", "templateItemEdges", "templateEdges"]),
    )
