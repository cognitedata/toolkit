"""Export of the instance-level resource dependency graph as YAML or JSON.

The data is already materialized during build: every ``BuiltResource`` records the
concrete resources it references in ``BuiltResource.dependencies``. This module only
serializes that graph and computes a topological ordering of the built resources.
"""

import json
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuildFolder, BuiltResource
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils import safe_write
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible, yaml_safe_dump

Node = tuple[type[ResourceIO], Identifier]


def _node_dict(crud_cls: type[ResourceIO], identifier: Identifier) -> dict[str, Any]:
    return {"type": crud_cls.kind, "id": str(identifier)}


def _sort_key(node: Node) -> tuple[str, str]:
    crud_cls, identifier = node
    return crud_cls.kind, str(identifier)


def _topological_order(graph: dict[Node, set[Node]], built_nodes: set[Node]) -> tuple[list[Node], list[list[Node]]]:
    """Topologically sort the built resources.

    Returns the ordered built nodes and any detected cycles. Cycles are broken so that a
    partial ordering can still be produced (mirrors the view-sorting behavior elsewhere).
    """
    cycles: list[list[Node]] = []
    working = {node: set(deps) for node, deps in graph.items()}
    while True:
        try:
            ordered = list(TopologicalSorter(working).static_order())
            break
        except CycleError as e:
            cycle_nodes: list[Node] = list(e.args[1])
            cycles.append(cycle_nodes)
            cycle_set = set(cycle_nodes)
            working = {n: (d - cycle_set) for n, d in working.items() if n not in cycle_set}
    return [node for node in ordered if node in built_nodes], cycles


def build_dependency_graph(build_folder: BuildFolder) -> dict[str, Any]:
    """Build the serializable instance-level dependency graph for a built folder."""
    built_nodes: set[Node] = {
        (resource.crud_cls, resource.identifier)
        for module in build_folder.built_modules
        for resource in module.resources
    }

    graph: dict[Node, set[Node]] = {}
    for module in build_folder.built_modules:
        for resource in module.resources:
            node: Node = (resource.crud_cls, resource.identifier)
            graph.setdefault(node, set()).update(resource.dependencies)

    ordered, cycles = _topological_order(graph, built_nodes)

    modules_output: list[dict[str, Any]] = []
    for module in build_folder.built_modules:
        if not module.resources:
            continue
        resources_output: list[dict[str, Any]] = []
        for resource in sorted(module.resources, key=lambda r: _sort_key((r.crud_cls, r.identifier))):
            entry: dict[str, Any] = {
                **_node_dict(resource.crud_cls, resource.identifier),
                "source": relative_to_if_possible(resource.source_path).as_posix(),
            }
            depends_on = _depends_on(resource, built_nodes)
            if depends_on:
                entry["depends_on"] = depends_on
            resources_output.append(entry)
        modules_output.append(
            {
                "module": str(module.module_id),
                "resources": resources_output,
            }
        )

    output: dict[str, Any] = {
        "topological_order": [
            {
                **_node_dict(crud_cls, identifier),
            }
            for crud_cls, identifier in ordered
        ],
        "modules": modules_output,
    }
    if cycles:
        output["cycles"] = [[_node_dict(crud_cls, identifier) for crud_cls, identifier in cycle] for cycle in cycles]
    return output


def _depends_on(resource: BuiltResource, built_nodes: set[Node]) -> list[dict[str, Any]]:
    depends_on: list[dict[str, Any]] = []
    for crud_cls, identifier in sorted(resource.dependencies, key=_sort_key):
        dependency = _node_dict(crud_cls, identifier)
        if (crud_cls, identifier) not in built_nodes:
            dependency["missing"] = True
        depends_on.append(dependency)
    return depends_on


def write_dependency_graph(build_folder: BuildFolder, output_path: Path) -> None:
    """Serialize the instance-level dependency graph to a YAML file."""
    graph = build_dependency_graph(build_folder)
    safe_write(output_path, yaml_safe_dump(graph, sort_keys=False))


def print_dependency_graph_json(build_folder: BuildFolder, indent: int = 2) -> None:
    """Print the instance-level dependency graph as JSON to stdout."""
    graph = build_dependency_graph(build_folder)
    print(json.dumps(graph, indent=indent))


__all__ = ["build_dependency_graph", "print_dependency_graph_json", "write_dependency_graph"]
