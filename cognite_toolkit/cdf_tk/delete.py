# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    ContainerList,
    DataModelList,
    SpaceApply,
    SpaceApplyList,
    ViewList,
)
from rich import print

from .utils import CDFToolConfig


def delete_instances(
    ToolGlobals: CDFToolConfig,
    space_name: str,
    dry_run: bool = False,
    delete_edges: bool = True,
    delete_nodes: bool = True,
) -> bool:
    """Delete instances in a space from CDF based on the space name

    Args:
        space_name (str): The name of the space to delete instances from
        dry_run (bool): Do not delete anything, just print what would have been deleted
        delete_edges (bool): Delete all edges in the space
        delete_nodes (bool): Delete all nodes in the space
    """
    if space_name is None or len(space_name) == 0:
        return True
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"[bold]Deleting instances in space {space_name}...[/]")
    if delete_edges:
        print("  Deleting edges...")
        # It's best practice to delete edges first as edges are deleted when nodes are deleted,
        # but this cascading delete is more expensive than deleting the edges directly.
        #
        # Find any edges in the space
        # Iterate over all the edges in the view 1,000 at the time
        edge_count = 0
        edge_delete = 0
        try:
            for instance_list in client.data_modeling.instances(
                instance_type="edge",
                include_typing=False,
                filter={"equals": {"property": ["edge", "space"], "value": space_name}},
                chunk_size=1000,
            ):
                instances = [(space_name, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(edges=instances)
                    edge_delete += len(ret.edges)
                edge_count += len(instance_list)
        except Exception as e:
            print(f"[bold red]ERROR: [/] Failed to delete edges in {space_name}.\n{e}")
            ToolGlobals.failed = True
            return False
        print(f"    Found {edge_count} edges and deleted {edge_delete} edges from space {space_name}.")
    if delete_nodes:
        print("  Deleting nodes...")
        # Find any nodes in the space
        node_count = 0
        node_delete = 0
        try:
            for node_list in client.data_modeling.instances(
                instance_type="node",
                include_typing=False,
                filter={"equals": {"property": ["node", "space"], "value": space_name}},
                chunk_size=1000,
            ):
                instances = [(space_name, i.external_id) for i in node_list]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(instances)
                    node_delete += len(ret.nodes)
                node_count += len(node_list)
        except Exception as e:
            print(f"[bold red]ERROR: [/] Failed to delete nodes in {space_name}.\n{e}")
            ToolGlobals.failed = True
            return False
        print(f"    Found {node_count} nodes and deleted {node_delete} nodes from {space_name}.")
    return True


def delete_containers_function(
    ToolGlobals: CDFToolConfig, dry_run: bool = False, containers: ContainerList | None = None
) -> None:
    if containers is None or len(containers) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.containers.delete(containers.as_ids())
            print(f"    Deleted {len(containers)} container(s).")
        else:
            print(f"    Would have deleted {len(containers)} container(s).")
    except Exception as e:
        print(f"    [bold yellow]WARNING: [/] Was not able to delete containers. May not exist.\n{e}")
        ToolGlobals.failed = True


def delete_views_function(ToolGlobals: CDFToolConfig, dry_run: bool = False, views: ViewList | None = None) -> None:
    if views is None or len(views) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.views.delete(views.as_ids())
            print(f"    Deleted {len(views.as_ids())} views.")
        else:
            print(f"    Would have deleted {len(views.as_ids())} views.")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete views.\n{e}")
        ToolGlobals.failed = True


def delete_spaces(ToolGlobals: CDFToolConfig, dry_run: bool = False, spaces: SpaceApplyList | None = None) -> None:
    if spaces is None or len(spaces) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.spaces.delete(spaces.as_ids())
            print(f"    Deleted {len(spaces.as_ids())} space(s).")
        else:
            print(f"    Would have deleted {len(spaces.as_ids())} space(s).")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete {len(spaces.as_ids())} space(s).\n{e}")
        ToolGlobals.failed = True
    return None


def delete_datamodels(
    ToolGlobals: CDFToolConfig, dry_run: bool = False, datamodels: DataModelList | None = None
) -> None:
    if datamodels is None or len(datamodels) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.data_models.delete(datamodels.as_ids())
            print(f"    Deleted {len(datamodels.as_ids())} data model(s).")
        else:
            print(f"    Would have deleted {len(datamodels.as_ids())} data model(s).")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete {len(datamodels.as_ids())} data model(s).\n{e}")
        ToolGlobals.failed = True


def delete_datamodel_all(
    ToolGlobals: CDFToolConfig,
    space_name: str | None = None,
    model_name: str | None = None,
    version: str | None = None,
    delete_edges: bool = True,
    delete_nodes: bool = True,
    delete_views: bool = True,
    delete_containers: bool = True,
    delete_space: bool = True,
    delete_datamodel: bool = True,
    dry_run: bool = False,
) -> None:
    """Delete data model from CDF based on the data model and space

    Args:
    space_name (str): The name of the space
    model_name (str): The name of the data model
    delete_edges (bool): Delete all edges defined by the model
    delete_nodes (bool): Delete all nodes defined by the model
    delete_views (bool): Delete all views in the data model
    delete_containers (bool): Delete all containers in the data model
    delete_space (bool): Delete the space
    delete_datamodel (bool): Delete the data model
    dry_run (bool): Do not delete anything, just print what would have been deleted
    """
    if space_name is None or model_name is None or version is None:
        raise ValueError("space_name, model_name, and version must be specified")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        data_model = client.data_modeling.data_models.retrieve((space_name, model_name, version))
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to retrieve data model {model_name}, version {version}.")
        print(e)
        return None
    if len(data_model) == 0:
        print(f"[bold red]ERROR: [/] Failed to retrieve data model {model_name}, version {version}.")
        view_list = ViewList([])
    else:
        views: ViewList = ViewList(data_model[0].views)

    print(f"[bold]Deleting {len(views.as_ids())} views in the data model {model_name}...[/]")
    try:
        containers = client.data_modeling.containers.list(space=space_name, limit=None)
    except Exception as e:
        print("[bold red]ERROR: [/] Failed to retrieve containers")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Deleting {len(containers.as_ids())} containers in the space {space_name}")
    if delete_nodes or delete_edges:
        delete_instances(
            ToolGlobals,
            space_name=space_name,
            dry_run=dry_run,
            delete_edges=delete_edges,
            delete_nodes=delete_nodes,
        )
    if delete_containers:
        delete_containers_function(ToolGlobals, dry_run=dry_run, containers=containers)
    if delete_views:
        delete_views_function(ToolGlobals, dry_run=dry_run, views=view_list)
    if delete_datamodel:
        delete_datamodels(ToolGlobals, dry_run=dry_run, datamodels=data_model)
    if delete_space:
        delete_spaces(
            ToolGlobals, dry_run=dry_run, spaces=SpaceApplyList([SpaceApply(space=space_name, name=space_name)])
        )


def clean_out_datamodels(ToolGlobals: CDFToolConfig, dry_run: bool = False, instances: bool = False) -> None:
    """WARNING!!!!

    Destructive: will delete all containers, views, data models, and spaces either
    found in local directory or GLOBALLY!!! (if not supplied)
    """
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
        print("Failed to retrieve everything needed.")
        print(e)
        ToolGlobals.failed = True
        return
    print("[bold]Deleting everything in data modeling for this project...[/]")
    print(f"  {len(spaces)} space(s)")
    print(f"  {len(containers)} container(s)")
    print(f"  {len(views)} view(s)")
    print(f"  {len(data_models)} data model(s)")
    print("Deleting...")
    delete_containers_function(ToolGlobals, dry_run=dry_run, containers=containers)
    delete_views_function(ToolGlobals, dry_run=dry_run, views=views)
    delete_datamodels(ToolGlobals, dry_run=dry_run, datamodels=data_models)
    for s in spaces:
        if instances:
            delete_instances(ToolGlobals, space_name=s.space, dry_run=dry_run)
        else:
            print(
                "[bold yellow]WARNING[/]Did not find --instances flag and will try to delete empty spaces without deleting remaining nodes and edges."
            )
    delete_spaces(ToolGlobals, dry_run=dry_run, spaces=spaces.as_apply())
    ToolGlobals.failed = False
