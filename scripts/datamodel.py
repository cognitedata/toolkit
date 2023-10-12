from __future__ import annotations

import datetime
import json
import os
from collections import defaultdict
from pathlib import Path
import re
from typing import Union
from dataclasses import dataclass

import yaml
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import View, ViewApply, SpaceApply, ContainerApply, DataModel, DataModelList, DataModelApply, ViewId, DirectRelationReference, DirectRelation
from cognite.client.exceptions import CogniteAPIError

from .delete import delete_datamodel

from .utils import CDFToolConfig


@dataclass
class Difference:
    added: list[CogniteResource]
    removed: list[CogniteResource]
    changed: list[CogniteResource]
    unchanged: list[CogniteResource]


def load_datamodel(ToolGlobals: CDFToolConfig, drop: bool, directory=None) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}"
    with open(f"{directory}/datamodel.graphql", "rt") as file:
        # Read directly into a string.
        datamodel = file.read()
    if drop:
        delete_datamodel(ToolGlobals, instances_only=False)
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    space_name = ToolGlobals.config("model_space")
    model_name = ToolGlobals.config("data_model")
    try:
        client.data_modeling.spaces.apply(
            SpaceApply(
                space=space_name,
                name=space_name,
                description=f"Space for {ToolGlobals.example} example",
            )
        )
    except Exception as e:
        print(f"Failed to write space {space_name} for example {ToolGlobals.example}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created space {space_name}.")
    try:
        client.data_modeling.graphql.apply_dml(
            (space_name, model_name, "1"),
            dml=datamodel,
            name=model_name,
            description=f"Data model for {ToolGlobals.example} example",
        )
    except Exception as e:
        print(
            f"Failed to write data model {model_name} to space {space_name} for example {ToolGlobals.example}."
        )
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created data model {model_name}.")


def clean_out_datamodels(
    ToolGlobals: CDFToolConfig, dry_run=False, directory=None, instances=False
) -> None:
    """WARNING!!!!

    Destructive: will delete all containers, views, data models, and spaces either
    found in local directory or GLOBALLY!!! (if not supplied)
    """
    if directory is not None:
        load_datamodel_dump(
            ToolGlobals, drop=True, directory=directory, dry_run=dry_run, only_drop=True
        )
        return
    print("WARNING: This will delete all data models, views, containers, and spaces.")
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        spaces = client.data_modeling.spaces.list(limit=-1)
        containers = client.data_modeling.containers.list(limit=-1)
        views = client.data_modeling.views.list(limit=-1, all_versions=True)
        data_models = client.data_modeling.data_models.list(limit=-1, all_versions=True)
    except Exception as e:
        print(f"Failed to retrieve everything needed.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Found:")
    print(f"  {len(spaces)} spaces")
    print(f"  {len(containers)} containers")
    print(f"  {len(views)} views")
    print(f"  {len(data_models)} data models")
    print("Deleting...")
    try:
        if not dry_run:
            client.data_modeling.containers.delete(
                [(c.space, c.external_id) for c in containers.data]
            )
        print(f"  Deleted {len(containers)} containers.")
    except Exception as e:
        print("  Was not able to delete containers. May not exist.")
        print(e)
    try:
        if not dry_run:
            client.data_modeling.views.delete(
                [(v.space, v.external_id, v.version) for v in views.data]
            )
        print(f"  Deleted {len(views)} views.")
    except Exception as e:
        print("  Was not able to delete views. May not exist.")
        print(e)
    try:
        if not dry_run:
            client.data_modeling.data_models.delete(
                [(d.space, d.external_id, d.version) for d in data_models.data]
            )
        print(f"  Deleted {len(data_models)} data models.")
    except Exception as e:
        print("  Was not able to delete data models. May not exist.")
        print(e)
    i = 0
    for s in spaces.data:
        if instances:
            print("Found --instances flag and will delete remaining nodes and edges.")
            # Find any remaining edges in the space
            edge_count = 0
            edge_delete = 0
            for instance_list in client.data_modeling.instances(
                instance_type="edge",
                include_typing=True,
                limit=None,
                filter={
                    "equals": {
                        "property": ["edge", "space"],
                        "value": s.space,
                    }
                },
                chunk_size=1000,
            ):
                instances = [(s.space, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(edges=instances)
                    edge_delete += len(ret.edges)
                edge_count += len(instance_list)
            print(
                f"Found {edge_count} edges and deleted {edge_delete} instances from {s.space}."
            )
            # Find any remaining nodes in the space
            node_count = 0
            node_delete = 0
            for instance_list in client.data_modeling.instances(
                instance_type="node",
                include_typing=True,
                limit=None,
                filter={
                    "equals": {
                        "property": ["node", "space"],
                        "value": s.space,
                    }
                },
                chunk_size=1000,
            ):
                instances = [(s.space, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(nodes=instances)
                    node_delete += len(ret.nodes)
                node_count += len(instance_list)
            print(
                f"Found {node_count} instances and deleted {node_delete} instances from {s.space}."
            )
        else:
            print(
                "Did not find --instances flag and will try to delete space without deleting remaining nodes and edges."
            )
        try:
            if not dry_run:
                client.data_modeling.spaces.delete(s.space)
            i += 1
        except Exception as e:
            print(f"  Was not able to delete space {s.space}.")
            print(e)
    print(f"  Deleted {i} spaces.")


def load_datamodel_dump(
    ToolGlobals: CDFToolConfig,
    drop: bool,
    directory: Path | None =None,
    dry_run: bool =False,
    only_drop: bool=False,
) -> None:
    if directory is None:
        directory = Path(f"./examples/{ToolGlobals.example}/data_model")
    model_files_by_type: dict[str, list[Path]] = defaultdict(list)
    models_pattern = re.compile(r"^(\w+\.)?(container|view|datamodel)\.yaml$")
    for file in directory.glob("**/*.yaml"):
        if not (match := models_pattern.match(file.name)):
            continue
        model_files_by_type[match.group(2)].append(file)
    for type_, files in model_files_by_type.items():
        print(f"Found {len(files)} {type_}s in {directory}.")

    cognite_resources_by_type: dict[str, list[Union[ContainerApply, ViewApply, DataModelApply, SpaceApply]]] = defaultdict(list)
    for type_, files in model_files_by_type.items():
        resource_cls = {
            "container": ContainerApply,
            "view": ViewApply,
            "datamodel": DataModelApply,
        }[type_]
        for file in files:
            cognite_resources_by_type[type_].append(
                resource_cls.load(yaml.safe_load(file.read_text()))
            )
    print("Loaded from files: ")
    for type_, resources in cognite_resources_by_type.items():
        print(f"  {type_}: {len(resources)}")

    space_list = list({r.space for _, resources in cognite_resources_by_type.items() for r in resources})

    print(f"Found {len(space_list)} spaces")
    cognite_resources_by_type["space"] = [SpaceApply(space=s, name=s, description="Imported space") for s in space_list]

    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )

    existing_resources_by_type: dict[str, list[Union[ContainerApply, ViewApply, DataModelApply, SpaceApply]]] = defaultdict(list)
    resource_api_by_type = {
        "container": client.data_modeling.containers,
        "view": client.data_modeling.views,
        "datamodel": client.data_modeling.data_models,
        "space": client.data_modeling.spaces,
    }
    for type_, resources in cognite_resources_by_type.items():
        existing_resources_by_type[type_] = resource_api_by_type[type_].retrieve([r.as_id() for r in resources])

    differences: dict[str, Difference] = {}
    for type_, resources in cognite_resources_by_type.items():
        new_by_id = {r.as_id(): r for r in resources}
        existing_by_id = {r.as_id(): r for r in existing_resources_by_type[type_]}

        added = [r for r in resources if r.as_id() not in existing_by_id]
        removed = [r for r in existing_resources_by_type[type_] if r.as_id() not in new_by_id]

        changed = []
        unchanged = []
        for existing_id in (set(new_by_id.keys()) & set(existing_by_id.keys())):
            if new_by_id[existing_id] == existing_by_id[existing_id]:
                unchanged.append(new_by_id[existing_id])
            else:
                changed.append(new_by_id[existing_id])

        differences[type_] = Difference(added, removed, changed, unchanged)

    creation_order = ["space", "container", "view", "datamodel"]

    if not only_drop:
        for type_ in creation_order:
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.added:
                print(f"Found {len(items.added)} new {type_}s.")
                if dry_run:
                    print(f"  Would create {len(items.added)} {type_}s.")
                    continue
                resource_api_by_type[type_].apply(items.added)
                print(f"  Created {len(items.added)} {type_}s.")
            if items.changed:
                print(f"Found {len(items.changed)} changed {type_}s.")
                if dry_run:
                    print(f"  Would update {len(items.changed)} {type_}s.")
                    continue
                resource_api_by_type[type_].apply(items.changed)
                print(f"  Updated {len(items.changed)} {type_}s.")
            if items.unchanged:
                print(f"Found {len(items.unchanged)} unchanged {type_}s.")

    if drop:
        for type_ in reversed(creation_order):
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.removed:
                print(f"Found {len(items.removed)} removed {type_}s.")
                if dry_run:
                    print(f"  Would delete {len(items.removed)} {type_}s.")
                    continue
                try:
                    resource_api_by_type[type_].delete(items.removed)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"  Failed to delete {len(items.removed)} {type_}s.")
                    print(e)
                    ToolGlobals.failed = True
                    continue
                print(f"  Deleted {len(items.removed)} {type_}s.")


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
        print(
            f"Found the space {space_name} with name ({space.name}) and description ({space.description})."
        )
        print(
            f"  - created_time: {datetime.datetime.fromtimestamp(space.created_time/1000)}"
        )
        print(
            f"  - last_updated_time: {datetime.datetime.fromtimestamp(space.last_updated_time/1000)}"
        )
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
        data_model = client.data_modeling.data_models.retrieve(
            (space_name, model_name, "1"), inline_views=True
        )
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
    print(
        f"  created_time: {datetime.datetime.fromtimestamp(data_model.data[0].created_time/1000)}"
    )
    print(
        f"  last_updated_time: {datetime.datetime.fromtimestamp(data_model.data[0].last_updated_time/1000)}"
    )
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
    print(f"------------------------------------------")

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
    print(
        f"Found in total {edge_count} edges in space {space_name} spread over {len(edge_relations)} types:"
    )
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
    print(
        f"Found in total {node_count} nodes in space {space_name} across all views and containers."
    )
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
        spaces = client.data_modeling.spaces.list(
            limit=None, include_global=include_global
        )
    except Exception as e:
        print(f"  Failed to retrieve all spaces.")
        print(e)
    spaces = spaces.data
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(
            limit=None, include_global=True
        )
    except Exception as e:
        print(f"Failed to retrieve all containers.")
        print(e)
        return
    containers = containers.data
    try:
        print("  views...")
        views = client.data_modeling.views.list(
            limit=None, space=None, include_global=include_global
        )
    except Exception as e:
        print(f"  Failed to retrieve all views.")
        print(e)
        return
    views = views.data
    try:
        print("  data models...")
        data_models: DataModelList = client.data_modeling.data_models.list(
            limit=-1, include_global=include_global, inline_views=True
        )
    except Exception as e:
        print(f"  Failed to retrieve all data models.")
        print(e)
        return
    data_models = data_models.data
    print("Writing...")
    for s in spaces:
        os.makedirs(f"{target_dir}/{s.space}")
    for d in data_models:
        with open(
            f"{target_dir}/{d.space}/{d.external_id}.model.json",
            "wt",
        ) as file:
            json.dump(d.dump(camel_case=True), file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.space}/{v.external_id}.view.json",
            "wt",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}//{c.space}/{c.external_id}.container.json",
            "wt",
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
        space = client.data_modeling.spaces.retrieve(space_name)
    except Exception as e:
        print(f"Failed to retrieve space {space_name}.")
        print(e)
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(
            space=space_name, limit=None, include_global=True
        )
    except Exception as e:
        print(f"Failed to retrieve containers for data model {model_name}.")
        print(e)
        return
    containers = containers.data
    try:
        print("  data model...")
        data_model = client.data_modeling.data_models.retrieve(
            (space_name, model_name, version), inline_views=False
        )
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
        print(
            f"{model_name} in space {space_name} does not have any views in this space."
        )
        views = []
    else:
        views = views.data[0].views
    print("Writing...")
    with open(
        f"{target_dir}/data_model.json",
        "wt",
    ) as file:
        json.dump(data_model, file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.external_id}.view.json",
            "wt",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}/{c.external_id}.container.json",
            "wt",
        ) as file:
            json.dump(c.dump(camel_case=True), file, indent=4)
