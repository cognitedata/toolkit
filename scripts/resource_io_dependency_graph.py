"""Visualize the dependencies between all concrete ResourceIO classes.

Usage:
    uv run --with pyvis python scripts/resource_io_dependency_graph.py [output.html]
"""

import sys
import webbrowser
from pathlib import Path

from pyvis.network import Network

from cognite_toolkit._cdf_tk import resource_ios  # noqa: F401 - ensures all ResourceIO subclasses are imported
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


def node_name(cls: type) -> str:
    return cls.__name__.removesuffix("IO")


def build_graph(output: Path) -> None:
    io_classes = get_concrete_subclasses(ResourceIO)  # type: ignore[type-abstract]
    net = Network(height="900px", width="100%", directed=True, cdn_resources="in_line")
    net.barnes_hut()

    for io_cls in io_classes:
        net.add_node(node_name(io_cls), label=node_name(io_cls), title=io_cls.__name__)

    for io_cls in io_classes:
        for dependency in io_cls.dependencies:
            if node_name(dependency) not in net.get_nodes():
                net.add_node(node_name(dependency), label=node_name(dependency), title=dependency.__name__)
            # Edge points from the dependency to the class that depends on it.
            net.add_edge(node_name(dependency), node_name(io_cls))

    output.write_text(net.generate_html(notebook=False), encoding="utf-8")
    print(f"Wrote {len(net.get_nodes())} nodes and {len(net.get_edges())} edges to {output.resolve()}")
    webbrowser.open(output.resolve().as_uri())


if __name__ == "__main__":
    build_graph(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("resource_io_dependencies.html"))
