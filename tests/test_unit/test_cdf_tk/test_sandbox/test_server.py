"""Tests for the sandbox HTTP server."""

import json
import shutil
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from cognite_toolkit._cdf_tk.dataflow import DataFlowNode, FlowKind, Snapshot, SnapshotEdge
from cognite_toolkit._cdf_tk.dataflow._io import save_snapshot
from cognite_toolkit._cdf_tk.sandbox._server import make_server
from tests.constants import REPO_ROOT

COMPLETE_ORG = REPO_ROOT / "tests/data/complete_org"
SQL_PATH = "modules/my_example_module/transformations/first.transformation.sql"


@pytest.fixture
def org_dir(tmp_path: Path) -> Path:
    dest = tmp_path / "complete_org"
    shutil.copytree(COMPLETE_ORG, dest)
    return dest


@pytest.fixture
def base_url() -> Iterator[str]:
    server = make_server("127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def call(base_url: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = Request(base_url + path, data=json.dumps(body).encode() if body is not None else None)
    with urlopen(request) as response:
        return json.load(response)


def load(base_url: str, org_dir: Path) -> Any:
    return call(
        base_url,
        "/load",
        {
            "organizationDir": str(org_dir),
            "configYaml": str(org_dir / "config.dev.yaml"),
            "modules": ["modules/my_example_module"],
        },
    )


def test_load_apply_diff_and_write(base_url: str, org_dir: Path) -> None:
    loaded = load(base_url, org_dir)
    assert loaded["counts"]["container"] >= 1
    assert loaded["counts"]["transformation"] >= 1
    assert call(base_url, "/diff") == {}

    result = call(
        base_url,
        "/apply",
        {
            "op": "SetPath",
            "kind": "Container",
            "identifier": {"space": "sp_schema", "externalId": "NameContainer"},
            "path": ["properties", "tag"],
            "value": {"type": {"type": "text"}},
        },
    )
    assert result["ok"]

    sql_path = org_dir / SQL_PATH
    query = sql_path.read_text().replace("cast(`externalId` as STRING) as externalId", "cast(`tag` as STRING) as tag")
    result = call(
        base_url,
        "/apply",
        {
            "op": "SetExtra",
            "kind": "Transformation",
            "identifier": {"externalId": "tr_first_transformation"},
            "field": "query",
            "text": query,
        },
    )
    assert result["ok"]

    diff = call(base_url, "/diff")
    assert {Path(file).name for file in diff} == {"name.Container.yaml", "first.transformation.sql"}

    written = call(base_url, "/write", {})["written"]
    assert len(written) == 2
    assert "as tag" in sql_path.read_text()
    assert "{{ example_variable }}" in sql_path.read_text()
    assert "tag:" in (org_dir / "modules/my_example_module/data_modeling/name.Container.yaml").read_text()


def test_get_resource_and_schema(base_url: str, org_dir: Path) -> None:
    load(base_url, org_dir)

    fetched = call(base_url, "/resources/Transformation?externalId=tr_first_transformation")
    assert fetched["resource"]["externalId"] == "tr_first_transformation"
    assert [extra["field"] for extra in fetched["extraFiles"]] == ["query"]
    assert "properties" in call(base_url, "/schema/Container")


def test_unknown_operation_returns_400(base_url: str, org_dir: Path) -> None:
    load(base_url, org_dir)
    with pytest.raises(HTTPError) as error:
        call(base_url, "/apply", {"op": "DropEverything"})
    assert error.value.code == 400


def test_describe_api(base_url: str) -> None:
    description = call(base_url, "/")
    assert "POST /apply" in description["routes"]
    assert set(description["operations"]) == {
        "SetPath",
        "RemovePath",
        "SetExtra",
        "AddResource",
        "RemoveResource",
    }
    assert "GET /cdf/diff" in description["routes"]


def test_load_with_missing_config_returns_400(base_url: str, org_dir: Path) -> None:
    with pytest.raises(HTTPError) as error:
        call(
            base_url,
            "/load",
            {"organizationDir": str(org_dir), "configYaml": "missing.yaml", "modules": ["modules/my_example_module"]},
        )
    assert error.value.code == 400


def test_rejected_token_value_gets_hint(base_url: str, org_dir: Path) -> None:
    load(base_url, org_dir)
    result = call(
        base_url,
        "/apply",
        {
            "op": "SetPath",
            "kind": "Container",
            "identifier": {"space": "sp_schema", "externalId": "NameContainer"},
            "path": ["properties", "tag"],
            "value": {"type": {"type": "text", "maxListSize": "{{ size }}"}},
        },
    )
    assert not result["ok"]
    assert "{{ variable }}" in result["hint"]


def test_dataflow_routes(base_url: str, tmp_path: Path) -> None:
    snapshot = Snapshot.create(
        [DataFlowNode(id=name, type="t/K", label=name) for name in "abc"],
        [
            SnapshotEdge(source="a", target="b", kind=FlowKind.READS),
            SnapshotEdge(source="b", target="c", kind=FlowKind.WRITES),
        ],
    )
    path = tmp_path / "snapshot.json"
    save_snapshot(snapshot, path)

    with pytest.raises(HTTPError) as error:
        call(base_url, "/dataflow")
    assert error.value.code == 400

    assert call(base_url, "/dataflow/load", {"path": str(path)})["source"] == "loaded"
    loaded = call(base_url, "/dataflow")
    assert loaded["source"] == "loaded"
    assert len(loaded["nodes"]) == 3

    neighbors = call(base_url, "/dataflow/neighbors?id=a&direction=out&depth=2&kinds=reads,writes")
    assert {node["id"] for node in neighbors["nodes"]} == {"a", "b", "c"}
    assert call(base_url, "/dataflow/node?id=b")["incoming"][0]["source"] == "a"
    assert "POST /dataflow/load" in call(base_url, "/")["routes"]


def test_dataflow_search_lineage_and_node_id_routes(base_url: str, org_dir: Path, tmp_path: Path) -> None:
    load(base_url, org_dir)
    resources = call(base_url, "/resources")
    transformation = next(item for item in resources if item["kind"] == "Transformation")
    assert transformation["nodeId"].startswith("transformations/Transformation/")

    snapshot = Snapshot.create(
        [
            DataFlowNode(
                id=transformation["nodeId"],
                type="transformations/Transformation",
                label="tr",
                folder="transformations",
                kind="Transformation",
            ),
            DataFlowNode(id="raw", type="external", label="raw"),
            DataFlowNode(id="view", type="data_modeling/View", label="view", folder="data_modeling", kind="View"),
        ],
        [
            SnapshotEdge(source=transformation["nodeId"], target="raw", kind=FlowKind.READS),
            SnapshotEdge(source=transformation["nodeId"], target="view", kind=FlowKind.WRITES),
        ],
    )
    path = tmp_path / "snapshot.json"
    save_snapshot(snapshot, path)
    call(base_url, "/dataflow/load", {"path": str(path)})

    page = call(base_url, "/dataflow/nodes?kind=transformation&limit=10")
    assert page["total"] == 1
    assert page["counts"] == {"transformations/Transformation": 1}

    identifier = transformation["identifier"]
    found = call(base_url, f"/dataflow/node-id?kind=Transformation&externalId={identifier['externalId']}")
    assert found["id"] == transformation["nodeId"]
    assert found["inSnapshot"]
    missing = call(base_url, "/dataflow/node-id?kind=Transformation&externalId=missing")
    assert not missing["inSnapshot"]

    lineage = call(base_url, "/dataflow/lineage?id=raw&only=processes")
    assert [step["id"] for step in lineage["downstream"]] == [transformation["nodeId"]]
    assert lineage["incomplete"] is False

    neighbors = call(base_url, "/dataflow/neighbors?id=raw&depth=2&limit=1")
    assert neighbors["truncated"]
    assert neighbors["totalNodes"] == 3
    assert [node["id"] for node in neighbors["nodes"]] == ["raw"]
    assert "GET /dataflow/lineage?id=&direction=&depth=&limit=&only=&orchestration=" in call(base_url, "/")["routes"]
