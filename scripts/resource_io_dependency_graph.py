"""Visualize the dependencies between all concrete ResourceIO classes.

Usage:
    uv run --with pyvis python scripts/resource_io_dependency_graph.py [output.html]
"""

import sys
import webbrowser
from collections import defaultdict
from pathlib import Path

from pyvis.network import Network

from cognite_toolkit._cdf_tk import resource_ios  # noqa: F401 - ensures all ResourceIO subclasses are imported
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

# Index = number of dependencies; the last color is used for that count and above.
DEPENDENCY_COUNT_COLORS = ["#2ca02c", "#1f77b4", "#ff7f0e", "#9467bd", "#d62728", "#8c564b", "#7f7f7f"]


TABLE_SCRIPT = """
<script>
  function filterDependencyTable(query) {
    query = query.toLowerCase();
    document.querySelectorAll("#dependency-table tbody tr").forEach(row => {
      row.style.display = row.textContent.toLowerCase().includes(query) ? "" : "none";
    });
  }
  function sortDependencyTable(column) {
    const table = document.getElementById("dependency-table");
    const ascending = table.dataset.sortColumn == column ? table.dataset.sortAscending !== "true" : true;
    table.dataset.sortColumn = column;
    table.dataset.sortAscending = ascending;
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    rows.sort((a, b) => {
      const x = a.cells[column].dataset.sort ?? a.cells[column].textContent;
      const y = b.cells[column].dataset.sort ?? b.cells[column].textContent;
      const result = isNaN(x) || isNaN(y) ? x.localeCompare(y) : Number(x) - Number(y);
      return ascending ? result : -result;
    });
    rows.forEach(row => tbody.appendChild(row));
  }
</script>
"""


def node_name(cls: type) -> str:
    return cls.__name__.removesuffix("IO")


def count_color(count: int) -> str:
    return DEPENDENCY_COUNT_COLORS[min(count, len(DEPENDENCY_COUNT_COLORS) - 1)]


def legend_html() -> str:
    last = len(DEPENDENCY_COUNT_COLORS) - 1
    rows = []
    for count, color in enumerate(DEPENDENCY_COUNT_COLORS):
        label = "0 (independent)" if count == 0 else f"{count} or more" if count == last else str(count)
        rows.append(
            f'<tr><td>{label}</td><td><span style="display:inline-block;width:14px;height:14px;'
            f'border-radius:50%;background:{color}"></span></td></tr>'
        )
    return (
        '<div style="position:fixed;top:10px;left:10px;z-index:1000;background:white;'
        'border:1px solid #ccc;border-radius:4px;padding:8px;font-family:sans-serif;font-size:13px">'
        '<table><tr><th style="padding-right:12px">Dependencies</th><th>Color</th></tr>'
        f"{''.join(rows)}</table></div>"
    )


def table_html(io_classes: list[type[ResourceIO]]) -> str:
    dependents: dict[str, list[str]] = defaultdict(list)
    for io_cls in io_classes:
        for dependency in io_cls.dependencies:
            dependents[node_name(dependency)].append(node_name(io_cls))

    rows = []
    for io_cls in sorted(io_classes, key=node_name):
        name = node_name(io_cls)
        count = len(io_cls.dependencies)
        depends_on = ", ".join(sorted(node_name(dependency) for dependency in io_cls.dependencies))
        depended_on_by = sorted(dependents[name])
        rows.append(
            f"<tr><td>{name}</td>"
            f'<td data-sort="{count}"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;'
            f'background:{count_color(count)};margin-right:6px"></span>{count}</td>'
            f"<td>{depends_on}</td>"
            f'<td data-sort="{len(depended_on_by)}">{", ".join(depended_on_by)}</td></tr>'
        )
    headers = ["Resource", "Dependencies", "Depends on", "Depended on by"]
    header_cells = "".join(
        f'<th style="cursor:pointer;text-align:left;border-bottom:2px solid #333;padding:4px 8px" '
        f'onclick="sortDependencyTable({i})">{header} &#8597;</th>'
        for i, header in enumerate(headers)
    )
    return (
        '<div style="font-family:sans-serif;font-size:13px;padding:16px">'
        "<h2>Resource dependencies</h2>"
        '<input type="search" placeholder="Filter..." oninput="filterDependencyTable(this.value)" '
        'style="margin-bottom:8px;padding:4px;width:300px">'
        '<table id="dependency-table" style="border-collapse:collapse;width:100%">'
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "<style>#dependency-table td{padding:4px 8px;border-bottom:1px solid #ddd;vertical-align:top}"
        "#dependency-table tbody tr:hover{background:#f5f5f5}</style>"
        f"{TABLE_SCRIPT}"
    )


def add_node(net: Network, io_cls: type[ResourceIO]) -> None:
    count = len(io_cls.dependencies)
    net.add_node(
        node_name(io_cls),
        label=node_name(io_cls),
        title=f"{io_cls.__name__} ({count} dependencies)",
        color=count_color(count),
    )


def build_graph(output: Path) -> None:
    io_classes = get_concrete_subclasses(ResourceIO)  # type: ignore[type-abstract]
    net = Network(height="900px", width="100%", directed=True, cdn_resources="in_line")
    net.barnes_hut()

    for io_cls in io_classes:
        add_node(net, io_cls)

    for io_cls in io_classes:
        for dependency in io_cls.dependencies:
            if node_name(dependency) not in net.get_nodes():
                add_node(net, dependency)
            # Edge points from the class to the dependency it requires.
            net.add_edge(node_name(io_cls), node_name(dependency))

    html = net.generate_html(notebook=False).replace("<body>", f"<body>\n{legend_html()}", 1)
    html = html.replace("</body>", f"{table_html(io_classes)}\n</body>", 1)
    output.write_text(html, encoding="utf-8")
    print(f"Wrote {len(net.get_nodes())} nodes and {len(net.get_edges())} edges to {output.resolve()}")
    webbrowser.open(output.resolve().as_uri())


if __name__ == "__main__":
    build_graph(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("resource_io_dependencies.html"))
