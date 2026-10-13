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
