from __future__ import annotations

import datetime

from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import (
    DirectRelation,
    DirectRelationReference,
)
from rich import print
from rich.table import Table

from .utils import CDFToolConfig


def describe_datamodel(ToolGlobals: CDFToolConfig, space_name: str, model_name: str) -> None:
    """Describe data model from CDF"""

    print(f"Describing data model ({model_name}) in space ({space_name})...")
    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    table = Table(title=f"Space {space_name}")
    table.add_column("Info", justify="left")
    table.add_column("Value", justify="left", style="green")
    try:
        space = client.data_modeling.spaces.retrieve(space_name)
    except Exception as e:
        print(f"Failed to retrieve space {space_name}.")
        print(e)
    else:
        if space is None:
            print(f"Failed to retrieve space {space_name}. It does not exists or you do not have access to it.")
        else:
            table.add_row("Name", str(space.name))
            table.add_row("Description", str(space.description))
            table.add_row("Created time", str(datetime.datetime.fromtimestamp(space.created_time / 1000)))
            table.add_row("Last updated time", str(datetime.datetime.fromtimestamp(space.last_updated_time / 1000)))
    try:
        containers = client.data_modeling.containers.list(space=space_name, include_global=True, limit=None)
    except Exception as e:
        print(f"Failed to retrieve containers for data model {model_name}.")
        print(e)
        return
    container_list = [(space_name, c.external_id) for c in containers.data]
    containers_str = []
    for c in container_list:
        containers_str.append(f"{c[1]}\n")
    if len(containers_str) > 0:
        containers_str[-1] = containers_str[-1][0:-1]
    table.add_row(f"Containers ({len(containers_str)})", "".join(containers_str))
    print(table)
    try:
        data_model = client.data_modeling.data_models.list(
            space=space_name, include_global=True, inline_views=True, limit=None
        )
    except Exception as e:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        print(e)
        return
    if len(data_model) == 0:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        return
    model_name = data_model.data[0].name
    if len(data_model.data) > 1:
        print(f"Found {len(data_model.data)} data models in space {space_name}.")
        print(f"  Only describing the first one ({model_name}).")
        print("  Use the --data-model flag to specify which data model to describe.")

    table = Table(title=f"Data model {model_name} in space {space_name}")
    table.add_column("Info", justify="right")
    table.add_column("Value", justify="left", style="green")
    table.add_row("Description", str(data_model.data[0].description))
    table.add_row(
        "Version",
        str(data_model.data[0].version),
    )
    table.add_row(
        "Global",
        "True" if data_model.data[0].is_global else "False",
    )
    table.add_row("Created time", str(datetime.datetime.fromtimestamp(data_model.data[0].created_time / 1000)))
    table.add_row(
        "Last updated time", str(datetime.datetime.fromtimestamp(data_model.data[0].last_updated_time / 1000))
    )
    views = data_model.data[0].views
    table.add_row("Number of views", str(len(views)))
    view_names = [f"{v.external_id}\n" for v in views]
    if len(view_names) > 0:
        view_names[-1] = view_names[-1][0:-1]
    table.add_row("List of views", "".join(view_names))
    print(table)
    for view in views:
        table = Table(title=f"View {view.external_id}, version {view.version} in space {space_name}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Number of properties", str(len(view.properties)))
        table.add_row("Used for", str(view.used_for))
        if view.implements is None:
            view.implements = []
        else:
            implements_str = [f"{i}\n" for i in view.implements]
        if len(implements_str) > 0:
            implements_str[-1] = implements_str[-1][0:-1]
        table.add_row("Implements", "".join(implements_str))
        properties = [f"{p}\n" for p in view.properties.keys()]
        if len(properties) > 0:
            properties[-1] = properties[-1][0:-1]
        table.add_row("List of properties", "".join(properties))
        direct_relations_str = []
        edge_relations_str = []
        nr_of_direct_relations = 0
        nr_of_edge_relations = 0
        for p, edge_type in view.properties.items():
            if type(edge_type.type) is DirectRelation:
                nr_of_direct_relations += 1
                if edge_type.source is None:
                    direct_relations_str.append(f"{p} --> no source")
                    continue
                direct_relations_str.append(
                    f"{p} --> ({edge_type.source.space}, {edge_type.source.external_id}, {edge_type.source.version})\n"
                )
            elif type(edge_type.type) is DirectRelationReference:
                nr_of_edge_relations += 1
                edge_relations_str.append(
                    f"{p} -- {edge_type.direction} --> ({edge_type.source.space}, {edge_type.source.external_id}, {edge_type.source.version})\n"
                )
        if len(direct_relations_str) > 0:
            direct_relations_str[-1] = direct_relations_str[-1][0:-1]
        if len(edge_relations_str) > 0:
            edge_relations_str[-1] = edge_relations_str[-1][0:-1]
        table.add_row(f"Direct relations({nr_of_direct_relations})", "".join(direct_relations_str))
        table.add_row(f"Edge relations({nr_of_edge_relations})", "".join(edge_relations_str))
        node_count = 0
        # Iterate over all the nodes in the view 1,000 at the time
        try:
            result = client.data_modeling.instances.aggregate(
                view.as_id(),
                aggregates=Count("externalId"),
                instance_type="node",
            )
            node_count = int(result.value)
        except Exception as e:
            print(f"Failed to retrieve nodes for view {view.external_id} version {view.version} in space {view.space}.")
            print(e)
        table.add_row("Number of nodes", f"{node_count:,}")
        edge_count = 0
        # Iterate over all the edges in the view 1,000 at the time
        try:
            result = client.data_modeling.instances.aggregate(
                view.as_id(),
                aggregates=Count("externalId"),
                instance_type="edge",
            )
            edge_count = int(result.value)
        except Exception as e:
            print(f"Failed to retrieve edges for view {view.external_id} version {view.version} in space {view.space}.")
            print(e)
        table.add_row("Number of edges", f"{edge_count:,}")
        print(table)
