"""Demos of the modeling sandbox server on tests/data/complete_org/modules/my_example_module.

The module has no transformation writing to a data model view, so a copy of it is seeded with one
(``seed/``) that populates ChildView. Each demo then edits the copy through the server's HTTP API.

    uv run python demos/sandbox/run_demo.py add-property
    uv run python demos/sandbox/run_demo.py remove-property
"""

import argparse
import json
import shutil
import tempfile
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from cognite_toolkit._cdf_tk.sandbox._server import make_server

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPLETE_ORG = REPO_ROOT / "tests/data/complete_org"
SEED = Path(__file__).parent / "seed"
MODULE = "modules/my_example_module"
TRANSFORMATION = {"externalId": "tr_child_view"}


def call(base_url: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = Request(base_url + path, data=json.dumps(body).encode() if body is not None else None)
    with urlopen(request) as response:
        return json.load(response)


def show(insights: list[dict[str, Any]]) -> None:
    """The complete_org module has many unrelated insights, so only the transformation check and errors are shown."""
    relevant = [
        i for i in insights if i.get("code") == "MISSING-REFERENCED-PROPERTY" or "Error" in i.get("insight_type", "")
    ]
    for insight in relevant:
        print(f"    {insight['code']}: {insight['message']}")
    if not relevant:
        print("    no transformation warnings")


def apply(base_url: str, title: str, operation: dict[str, Any]) -> None:
    result = call(base_url, "/apply", operation)
    print(f"\n> {title}  [{'ok' if result['ok'] else 'rejected'}]")
    show(result["insights"])


def query_text(base_url: str) -> str:
    resource = call(base_url, "/resources/Transformation?externalId=tr_child_view")
    return next(extra["text"] for extra in resource["extraFiles"] if extra["field"] == "query")


def set_query(base_url: str, title: str, edit: Callable[[str], str]) -> None:
    apply(
        base_url,
        title,
        {
            "op": "SetExtra",
            "kind": "Transformation",
            "identifier": TRANSFORMATION,
            "field": "query",
            "text": edit(query_text(base_url)),
        },
    )


def add_property(base_url: str) -> None:
    # The transformation is edited first on purpose: the rule warns until the view has the property.
    set_query(
        base_url,
        "Select the new column 'category' in the transformation",
        lambda sql: sql.replace(
            "  cast(`description` as STRING) as description\n",
            "  cast(`description` as STRING) as description,\n  cast(`category` as STRING) as category\n",
        ),
    )
    apply(
        base_url,
        "Add 'category' to DescriptionContainer",
        {
            "op": "SetPath",
            "kind": "Container",
            "identifier": {"space": "sp_schema", "externalId": "DescriptionContainer"},
            "path": ["properties", "category"],
            "value": {"nullable": True, "type": {"type": "text", "list": False}},
        },
    )
    apply(
        base_url,
        "Expose 'category' in ChildView",
        {
            "op": "SetPath",
            "kind": "View",
            "identifier": {"space": "sp_schema", "externalId": "ChildView", "version": "v1"},
            "path": ["properties", "category"],
            "value": {
                "container": {"space": "sp_schema", "externalId": "DescriptionContainer", "type": "container"},
                "containerPropertyIdentifier": "category",
            },
        },
    )


def remove_property(base_url: str) -> None:
    # ChildView inherits 'name' from ParentView, so removing it there leaves the transformation selecting it.
    apply(
        base_url,
        "Remove 'name' from ParentView",
        {
            "op": "RemovePath",
            "kind": "View",
            "identifier": {"space": "sp_schema", "externalId": "ParentView", "version": "v1"},
            "path": ["properties", "name"],
        },
    )
    set_query(
        base_url,
        "Stop selecting 'name' in the transformation",
        lambda sql: sql.replace("  cast(`name` as STRING) as name,\n", ""),
    )
    apply(
        base_url,
        "Remove 'name' from NameContainer",
        {
            "op": "RemovePath",
            "kind": "Container",
            "identifier": {"space": "sp_schema", "externalId": "NameContainer"},
            "path": ["properties", "name"],
        },
    )


DEMOS: dict[str, Callable[[str], None]] = {"add-property": add_property, "remove-property": remove_property}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("demo", choices=sorted(DEMOS))
    args = parser.parse_args()

    org_dir = Path(tempfile.mkdtemp(prefix="sandbox_demo_")).resolve() / "complete_org"
    shutil.copytree(COMPLETE_ORG, org_dir)
    shutil.copytree(SEED, org_dir, dirs_exist_ok=True)
    print(f"Working copy: {org_dir}")

    server = make_server("127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        call(
            base_url,
            "/load",
            {"organizationDir": str(org_dir), "configYaml": str(org_dir / "config.dev.yaml"), "modules": [MODULE]},
        )
        print("\nInsights after load:")
        show(call(base_url, "/insights")["insights"])

        DEMOS[args.demo](base_url)

        print("\nDiff:")
        for file, diff in call(base_url, "/diff").items():
            print(f"\n--- {Path(file).relative_to(org_dir)}\n{diff}")
        written = call(base_url, "/write", {})["written"]
        print(f"\nWrote {len(written)} files to the working copy.")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
