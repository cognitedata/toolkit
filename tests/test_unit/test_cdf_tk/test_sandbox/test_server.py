"""Tests for the sandbox HTTP server and the transformation SQL support."""

import json
import shutil
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ExternalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewCorePropertyRequest, ViewRequest
from cognite_toolkit._cdf_tk.sandbox._server import make_server
from cognite_toolkit._cdf_tk.sandbox._source import SqlSource
from cognite_toolkit._cdf_tk.sandbox._validation import transformation_alias_insights
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


def test_load_apply_diff_and_write(base_url: str, org_dir: Path) -> None:
    loaded = call(
        base_url,
        "/load",
        {
            "organizationDir": str(org_dir),
            "configYaml": str(org_dir / "config.dev.yaml"),
            "modules": ["modules/my_example_module"],
        },
    )
    assert loaded["counts"]["container"] >= 1
    assert loaded["counts"]["transformationQuery"] == 1
    assert call(base_url, "/diff") == {}

    result = call(
        base_url,
        "/apply",
        {
            "op": "AddContainerProperty",
            "container": {"space": "sp_schema", "externalId": "NameContainer"},
            "name": "tag",
            "definition": {"type": {"type": "text"}},
        },
    )
    assert result["ok"]

    sql_path = org_dir / SQL_PATH
    query = sql_path.read_text().replace("cast(`externalId` as STRING) as externalId", "cast(`tag` as STRING) as tag")
    result = call(
        base_url,
        "/apply",
        {"op": "SetTransformationQuery", "transformation": {"externalId": "tr_first_transformation"}, "query": query},
    )
    assert result["ok"]

    diff = call(base_url, "/diff")
    assert {Path(file).name for file in diff} == {"name.Container.yaml", "first.transformation.sql"}

    written = call(base_url, "/write", {})["written"]
    assert len(written) == 2
    assert "as tag" in sql_path.read_text()
    assert "{{ example_variable }}" in sql_path.read_text()
    assert "tag:" in (org_dir / "modules/my_example_module/data_modeling/name.Container.yaml").read_text()


def test_unknown_operation_returns_400(base_url: str, org_dir: Path) -> None:
    call(
        base_url,
        "/load",
        {
            "organizationDir": str(org_dir),
            "configYaml": str(org_dir / "config.dev.yaml"),
            "modules": ["modules/my_example_module"],
        },
    )
    with pytest.raises(HTTPError) as error:
        call(base_url, "/apply", {"op": "DropEverything"})
    assert error.value.code == 400


def test_transformation_alias_warning_clears_when_view_has_property(tmp_path: Path) -> None:
    view_id = ViewId(space="sp", external_id="Person", version="v1")
    sql = SqlSource(
        path=tmp_path / "t.sql",
        original_text="",
        text="-- from comment\nSELECT\n  p.externalId AS externalId,\n  node_reference('a', b.externalId) AS rel,\n  b.dob AS dateOfBirth\nFROM x AS p",
        variables=[],
        transformation=ExternalId(external_id="t"),
        destination_view=view_id,
    )

    def view_with(*names: str) -> ViewRequest:
        return ViewRequest(
            space="sp",
            external_id="Person",
            version="v1",
            properties={
                name: ViewCorePropertyRequest(
                    container=ContainerId(space="sp", external_id="Person"), container_property_identifier=name
                )
                for name in names
            },
        )

    assert transformation_alias_insights({sql.transformation: sql}, {view_id: view_with("rel", "dateOfBirth")}) == []

    insights = transformation_alias_insights({sql.transformation: sql}, {view_id: view_with("rel")})
    assert len(insights) == 1
    assert "dateOfBirth" in insights[0].message
