from __future__ import annotations

import datetime
import json
import os
import tempfile
from collections.abc import Sequence

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DataModelList,
    DirectRelation,
    DirectRelationReference,
    ViewId,
)

from .utils import CDFToolConfig


def describe_datamodel(ToolGlobals: CDFToolConfig, space_name, model_name) -> None:
    """Describe data model from CDF"""

    print("Describing data model ({model_name}) in space ({space_name})...")
    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        space = client.data_modeling.spaces.retrieve(space_name)
        print(f"Found the space {space_name} with name ({space.name}) and description ({space.description}).")
        print(f"  - created_time: {datetime.datetime.fromtimestamp(space.created_time/1000)}")
        print(f"  - last_updated_time: {datetime.datetime.fromtimestamp(space.last_updated_time/1000)}")
    except Exception as e:
        print(f"Failed to retrieve space {space_name}.")
        print(e)
    try:
        containers = client.data_modeling.containers.list(space=space_name, limit=None)
    except Exception as e:
        print(f"Failed to retrieve containers for data model {model_name}.")
        print(e)
        return
    container_list = [(space_name, c.external_id) for c in containers.data]
    print(f"Found {len(container_list)} containers in the space {space_name}:")
    for c in container_list:
        print(f"  {c[1]}")
    try:
        data_model = client.data_modeling.data_models.retrieve((space_name, model_name, "1"), inline_views=True)
    except Exception as e:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        print(e)
        return
    if len(data_model) == 0:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        return
    print(f"Found data model {model_name} in space {space_name}:")
    print(f"  version: {data_model.data[0].version}")
    print(f"  global: {'True' if data_model.data[0].is_global else 'False'}")
    print(f"  description: {data_model.data[0].description}")
    print(f"  created_time: {datetime.datetime.fromtimestamp(data_model.data[0].created_time/1000)}")
    print(f"  last_updated_time: {datetime.datetime.fromtimestamp(data_model.data[0].last_updated_time/1000)}")
    views = data_model.data[0].views
    print(f"  {model_name} has {len(views)} views:")
    direct_relations = 0
    edge_relations = 0
    for v in views:
        print(f"    {v.external_id}, version: {v.version}")
        print(f"       - properties: {len(v.properties)}")
        print(f"       - used for {v.used_for}s")
        print(f"       - implements: {v.implements}")
        for p, d in v.properties.items():
            if type(d.type) is DirectRelation:
                direct_relations += 1
                if d.source is None:
                    print(f"{p} has no source")
                    continue
                print(
                    f"       - direct relation 1:1 {p} --> ({d.source.space}, {d.source.external_id}, {d.source.version})"
                )
            elif type(d.type) is DirectRelationReference:
                edge_relations += 1
                print(
                    f"       - edge relation 1:MANY {p} -- {d.direction} --> ({d.source.space}, {d.source.external_id}, {d.source.version})"
                )

    print(f"Total direct relations: {direct_relations}")
    print(f"Total edge relations: {edge_relations}")
    print("------------------------------------------")

    # Find any edges in the space
    # Iterate over all the edges in the view 1,000 at the time
    edge_count = 0
    edge_relations = {}
    for instance_list in client.data_modeling.instances(
        instance_type="edge",
        include_typing=False,
        filter={"equals": {"property": ["edge", "space"], "value": space_name}},
        chunk_size=1000,
    ):
        for i in instance_list.data:
            if type(i.type) is DirectRelationReference:
                if edge_relations.get(i.type.external_id) is None:
                    edge_relations[i.type.external_id] = 0
                edge_relations[i.type.external_id] += 1
        edge_count += len(instance_list.data)
    sum = 0
    for count in edge_relations.values():
        sum += count
    print(f"Found in total {edge_count} edges in space {space_name} spread over {len(edge_relations)} types:")
    for d, c in edge_relations.items():
        print(f"  {d}: {c}")
    print("------------------------------------------")
    # Find all nodes in the space
    node_count = 0
    for instance_list in client.data_modeling.instances(
        instance_type="node",
        include_typing=False,
        filter={"equals": {"property": ["node", "space"], "value": space_name}},
        chunk_size=1000,
    ):
        node_count += len(instance_list)
    print(f"Found in total {node_count} nodes in space {space_name} across all views and containers.")
    # For all the views in this data model...
    for v in views:
        node_count = 0
        # Iterate over all the nodes in the view 1,000 at the time
        for instance_list in client.data_modeling.instances(
            instance_type="node",
            include_typing=False,
            sources=ViewId(space_name, v.external_id, v.version),
            chunk_size=1000,
        ):
            node_count += len(instance_list)
        print(f"  {node_count} nodes of view {v.external_id}.")


def dump_datamodels_all(
    ToolGlobals: CDFToolConfig,
    target_dir: str = "tmp",
    include_global: bool = False,
):
    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        print("  spaces...")
        spaces = client.data_modeling.spaces.list(limit=None, include_global=include_global)
    except Exception as e:
        print("  Failed to retrieve all spaces.")
        print(e)
    spaces = spaces.data
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(limit=None, include_global=True)
    except Exception as e:
        print("Failed to retrieve all containers.")
        print(e)
        return
    containers = containers.data
    try:
        print("  views...")
        views = client.data_modeling.views.list(limit=None, space=None, include_global=include_global)
    except Exception as e:
        print("  Failed to retrieve all views.")
        print(e)
        return
    views = views.data
    try:
        print("  data models...")
        data_models: DataModelList = client.data_modeling.data_models.list(
            limit=-1, include_global=include_global, inline_views=True
        )
    except Exception as e:
        print("  Failed to retrieve all data models.")
        print(e)
        return
    data_models = data_models.data
    print("Writing...")
    for s in spaces:
        os.makedirs(f"{target_dir}/{s.space}")
    for d in data_models:
        with open(
            f"{target_dir}/{d.space}/{d.external_id}.model.json",
            "w",
        ) as file:
            json.dump(d.dump(camel_case=True), file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.space}/{v.external_id}.view.json",
            "w",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}//{c.space}/{c.external_id}.container.json",
            "w",
        ) as file:
            json.dump(c.dump(camel_case=True), file, indent=4)


def dump_datamodel(
    ToolGlobals: CDFToolConfig,
    space_name,
    model_name,
    version: str = "1",
    target_dir: str = "tmp",
) -> None:
    """Dump data model from CDF"""

    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"Loading data model ({model_name}) in space ({space_name})...")
    try:
        print("  space...")
        client.data_modeling.spaces.retrieve(space_name)
    except Exception as e:
        print(f"Failed to retrieve space {space_name}.")
        print(e)
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(space=space_name, limit=None, include_global=True)
    except Exception as e:
        print(f"Failed to retrieve containers for data model {model_name}.")
        print(e)
        return
    containers = containers.data
    try:
        print("  data model...")
        data_model = client.data_modeling.data_models.retrieve((space_name, model_name, version), inline_views=False)
        data_model = data_model.data[0].dump(camel_case=True)
    except Exception as e:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        print(e)
        return
    if len(data_model) == 0:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        return
    try:
        print("  views...")
        views = client.data_modeling.views.retrieve((space_name, model_name, None))
    except Exception as e:
        print(f"Failed to retrieve views from {model_name} in space {space_name}.")
        print(e)
        return
    if len(views.data) == 0:
        print(f"{model_name} in space {space_name} does not have any views in this space.")
        views = []
    else:
        views = views.data[0].views
    print("Writing...")
    with open(
        f"{target_dir}/data_model.json",
        "w",
    ) as file:
        json.dump(data_model, file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.external_id}.view.json",
            "w",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}/{c.external_id}.container.json",
            "w",
        ) as file:
            json.dump(c.dump(camel_case=True), file, indent=4)


def dump_transformations(
    ToolGlobals: CDFToolConfig,
    external_ids: Sequence[str] | None = None,
    target_dir: str | None = None,
    ignore_unknown_ids: bool = True,
):
    """Dump transformations from CDF"""

    print("Verifying access rights...")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "transformationsAcl": ["READ"],
        }
    )
    print(f"Loading {len(external_ids) if external_ids else 'all'} transformations...")
    try:
        if external_ids is None:
            transformations = client.transformations.list()
        else:
            transformations = client.transformations.retrieve_multiple(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
    except Exception as e:
        print("Failed to retrieve transformations.")
        print(e)
        return
    # Clean up and write
    print("Writing...")
    for t in transformations:
        t2 = t.dump()
        try:
            t2.pop("id")
            t2.pop("created_time")
            t2.pop("last_updated_time")
            t2.pop("owner")
            t2.pop("last_finished_job")
            t2.pop("source_session")
            t2.pop("destination_session")
        except KeyError:
            ...
        query = t2.pop("query")
        with open(
            f"{target_dir}/{t2.get('external_id') or tempfile.TemporaryFile(dir=target_dir).name}.json",
            "w",
        ) as file:
            json.dump(t2, file, indent=2)
        with open(
            f"{target_dir}/{t2.get('external_id') or tempfile.TemporaryFile(dir=target_dir).name}.sql",
            "w",
        ) as file:
            for line in query.splitlines():
                file.write(line + "\n")
    print(f"Done writing {len(transformations)} transformations to {target_dir}.")
