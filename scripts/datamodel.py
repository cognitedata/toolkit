import datetime
import json
import os
from cognite.client.data_classes.data_modeling import (
    ViewId,
    DirectRelationReference,
    DirectRelation,
)
from cognite.client.data_classes.data_modeling.data_models import (
    DataModel,
    DataModelList,
)
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.data_modeling.spaces import SpaceApply
from cognite.client.data_classes.data_modeling.containers import Container
from .delete import delete_datamodel

from .utils import CDFToolConfig


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
    directory=None,
    dry_run=False,
    only_drop=False,
) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/data_model"
    model_files = []
    # Pick up all the datamodels.
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if "model.json" in f:
                model_files.append(f"{dirpath}/{f}")
    print(f"Found {len(model_files)} data models in {directory}.")
    view_files = []
    # Pick up all the views.
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if "view.json" in f:
                view_files.append(f"{dirpath}/{f}")
    print(f"Found {len(view_files)} views in {directory}.")
    container_files = []
    # Pick up all the containers.
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if "container.json" in f:
                container_files.append(f"{dirpath}/{f}")
    print(f"Found {len(container_files)} containers in {directory}.")
    containers = []
    for f in container_files:
        with open(f"{f}", "rt") as file:
            # Load container and convert to apply (write) version of view as we are reading
            # in a dump from a file.
            # The dump is from API /models/containers
            containers.append(Container.load(json.load(file)).as_apply())
    views = []
    for f in view_files:
        with open(f"{f}", "rt") as file:
            # Load view and convert to apply (write) version of view as we are reading
            # in a dump from a file.
            # The dump is from API /models/views/byids.
            views.append(View.load(json.load(file)).as_apply())
    datamodels = []
    for f in model_files:
        with open(f"{f}", "rt") as file:
            # Load view and convert to apply (write) version of view as we are reading
            # in a dump from a file.
            # The dump is from API /models/views/byids.
            datamodels.append(DataModel.load(json.load(file)).as_apply())
    print("Loaded from files: ")
    print(f"  {len(containers)} containers")
    print(f"  {len(views)} views")
    print(f"  {len(datamodels)} data models")
    space_list = []
    container_list = []
    view_list = []
    model_list = []
    for v in datamodels:
        if v.space not in space_list:
            space_list.append(v.space)
        if (v.space, v.external_id) not in model_list:
            model_list.append((v.space, v.external_id))
    for v in views:
        if v.space not in space_list:
            space_list.append(v.space)
        if (v.space, v.external_id) not in view_list:
            view_list.append((v.space, v.external_id))
    for c in containers:
        if c.space not in space_list:
            space_list.append(c.space)
        if (v.space, v.external_id) not in container_list:
            container_list.append((v.space, v.external_id))
    print(f"Found {len(space_list)} spaces")
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    if dry_run:
        return
    if drop:
        print("Deleting...")
        try:
            client.data_modeling.containers.delete([c for c in containers])
            print(f"  Deleted {len(containers)} containers.")
        except:
            print("  Was not able to delete containers. May not exist.")
        try:
            client.data_modeling.views.delete([v for v in views])
            print(f"  Deleted {len(views)} views.")
        except:
            print("  Was not able to delete views. May not exist.")
        try:
            client.data_modeling.data_models.delete([d for d in datamodels])
            print(f"  Deleted {len(datamodels)} data models.")
        except:
            print("  Was not able to delete data models. May not exist.")
        try:
            client.data_modeling.spaces.delete([d for d in space_list])
            print(f"  Deleted {len(space_list)} spaces.")
        except:
            print("  Was not able to delete spaces. May not exist.")
        if only_drop:
            return
    print("Writing...")
    try:
        client.data_modeling.spaces.apply(
            [
                SpaceApply(space=s, name=s, description=f"Imported space")
                for s in space_list
            ]
        )
    except Exception as e:
        print(f"  Failed to write spaces")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Created {len(space_list)} spaces.")
    try:
        client.data_modeling.containers.apply([c for c in containers])
        print(f"  Created {len(containers)} containers.")
    except Exception as e:
        print(f"  Failed to write containers.")
        print(e)
        ToolGlobals.failed = True
        return
    try:
        client.data_modeling.views.apply([v for v in views])
        print(f"  Created {len(views)} views.")
    except Exception as e:
        print(f"  Failed to write views.")
        print(e)
        ToolGlobals.failed = True
        return
    try:
        client.data_modeling.data_models.apply([d for d in datamodels])
    except Exception as e:
        print(f"  Failed to write data models.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Created {len(datamodels)} data models.")


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
