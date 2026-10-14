"""Tests for YAML and CDF resource stores in the sandbox."""

import json
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.sandbox import RemoveResource, SetPath
from cognite_toolkit._cdf_tk.sandbox._server import make_server
from cognite_toolkit._cdf_tk.sandbox._workspace import Workspace
from tests.test_unit.approval_client import ApprovalToolkitClient

MODULE = "modules/mod"


def _minimal_org(tmp_path: Path) -> Path:
    org = tmp_path / "org"
    module = org / MODULE / "data_modeling"
    module.mkdir(parents=True)
    (org / "config.dev.yaml").write_text(
        "environment:\n  name: dev\n  project: p\n  type: dev\n  selected:\n    - modules/\n"
        "variables:\n  modules:\n    mod:\n      prefix: sp\n"
    )
    (module / "schema.Space.yaml").write_text("space: '{{ prefix }}_schema'\n")
    (module / "pump.Container.yaml").write_text(
        "space: '{{ prefix }}_schema'\n"
        "externalId: Pump\n"
        "usedFor: node\n"
        "properties:\n"
        "  name:\n"
        "    type:\n"
        "      type: text\n"
    )
    return org


@pytest.fixture
def org_dir(tmp_path: Path) -> Path:
    return _minimal_org(tmp_path)


def call(base_url: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = Request(base_url + path, data=json.dumps(body).encode() if body is not None else None)
    with urlopen(request) as response:
        return json.load(response)


@pytest.fixture
def cdf_server(toolkit_client_approval: ApprovalToolkitClient) -> Iterator[tuple[str, ApprovalToolkitClient]]:
    server = make_server("127.0.0.1", 0, client=toolkit_client_approval.client)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", toolkit_client_approval
    server.shutdown()
    server.server_close()


def test_cdf_diff_reports_create_and_unchanged(
    org_dir: Path, toolkit_client_approval: ApprovalToolkitClient
) -> None:
    toolkit_client_approval.append(
        SpaceResponse,
        SpaceResponse(space="sp_schema", is_global=False, created_time=1, last_updated_time=1),
    )
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])

    plan = workspace.cdf_plan().to_json()
    space_plan = next(kind for kind in plan["kinds"] if kind["kind"] == "Space")
    assert "sp_schema" in space_plan["unchanged"]
    container_plan = next(kind for kind in plan["kinds"] if kind["kind"] == "Container")
    assert any("Pump" in item for item in container_plan["toCreate"])


def test_cdf_deploy_dry_run_does_not_create(
    org_dir: Path, toolkit_client_approval: ApprovalToolkitClient
) -> None:
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])

    result = workspace.cdf_deploy(dry_run=True, confirm=False).to_json()
    assert result["dryRun"] is True
    assert toolkit_client_approval.created_resources == {}


def test_cdf_deploy_requires_confirm(org_dir: Path, toolkit_client_approval: ApprovalToolkitClient) -> None:
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])
    with pytest.raises(ValueError, match="confirm"):
        workspace.cdf_deploy(dry_run=False, confirm=False)


def test_cdf_load_dumps_space_into_session(
    org_dir: Path, toolkit_client_approval: ApprovalToolkitClient
) -> None:
    toolkit_client_approval.append(
        SpaceResponse,
        SpaceResponse(
            space="from_cdf",
            name="From CDF",
            is_global=False,
            created_time=1,
            last_updated_time=1,
        ),
    )
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])

    loaded = workspace.cdf_load(
        {"kind": "Space", "folder": "data_modeling", "identifiers": [{"space": "from_cdf"}]}
    )
    assert loaded["added"] == [{"kind": "Space", "identifier": {"space": "from_cdf"}}]
    written = workspace.get_session().write()
    dumped = next(path for path in written if path.name.endswith("from_cdf.Space.yaml") or "from_cdf" in path.name)
    assert "space: from_cdf" in dumped.read_text()
    assert "name: From CDF" in dumped.read_text()


def test_changes_compare_cdf_dump_against_its_loaded_text(
    org_dir: Path, toolkit_client_approval: ApprovalToolkitClient
) -> None:
    toolkit_client_approval.append(
        SpaceResponse,
        SpaceResponse(space="from_cdf", name="From CDF", is_global=False, created_time=1, last_updated_time=1),
    )
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])
    workspace.cdf_load({"kind": "Space", "folder": "data_modeling", "identifiers": [{"space": "from_cdf"}]})
    session = workspace.get_session()

    def dumped() -> dict[str, Any]:
        return next(entry for entry in session.changes() if "from_cdf" in entry["path"])

    assert dumped()["origin"] == "cdf"
    assert dumped()["state"] == "unchanged"
    assert dumped()["baseline"] == dumped()["current"]
    # The disk diff calls the whole file new, which is what changes() avoids.
    assert any("from_cdf" in str(path) for path in session.diff())

    edit = SetPath(kind="Space", identifier={"space": "from_cdf"}, path=["description"], value="edited")
    result = session.apply(edit)
    assert result.ok, result.insights
    assert dumped()["state"] == "changed"
    assert "description" not in dumped()["baseline"]
    assert "description: edited" in dumped()["current"]


def test_changes_leave_out_untouched_disk_files(org_dir: Path) -> None:
    workspace = Workspace()
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])
    session = workspace.get_session()
    assert session.changes() == []

    session.apply(SetPath(kind="Space", identifier={"space": "sp_schema"}, path=["description"], value="x"))
    [entry] = session.changes()
    assert entry["origin"] == "sandbox"
    assert entry["state"] == "changed"


def test_cdf_pull_merges_description(
    org_dir: Path, toolkit_client_approval: ApprovalToolkitClient
) -> None:
    toolkit_client_approval.append(
        SpaceResponse,
        SpaceResponse(
            space="sp_schema",
            description="pulled from cdf",
            is_global=False,
            created_time=1,
            last_updated_time=1,
        ),
    )
    workspace = Workspace(client=toolkit_client_approval.client)
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])

    result = workspace.cdf_pull({"kind": "Space", "identifier": {"space": "sp_schema"}})
    assert result["updated"]
    space = workspace.get_resource("Space", {"space": "sp_schema"})["resource"]
    assert space["description"] == "pulled from cdf"
    [entry] = [e for e in workspace.get_session().changes() if "schema.Space" in e["path"]]
    assert (entry["origin"], entry["state"]) == ("cdf", "unchanged")
    workspace.get_session().write()
    text = (org_dir / MODULE / "data_modeling" / "schema.Space.yaml").read_text()
    assert "pulled from cdf" in text
    assert "{{ prefix }}_schema" in text


def test_remove_resource_deletes_file_on_write(org_dir: Path) -> None:
    workspace = Workspace()
    workspace.load(org_dir, org_dir / "config.dev.yaml", [MODULE])
    path = (org_dir / MODULE / "data_modeling" / "schema.Space.yaml").resolve()
    assert path.exists()

    result = workspace.get_session().apply(RemoveResource(kind="Space", identifier={"space": "sp_schema"}))
    assert result.ok, result.insights
    assert path in workspace.get_session().diff()
    written = workspace.get_session().write()
    assert path in written
    assert not path.exists()


def test_cdf_routes_on_server(cdf_server: tuple[str, ApprovalToolkitClient], org_dir: Path) -> None:
    base_url, approval = cdf_server
    approval.append(
        SpaceResponse,
        SpaceResponse(space="sp_schema", is_global=False, created_time=1, last_updated_time=1),
    )
    call(
        base_url,
        "/load",
        {
            "organizationDir": str(org_dir),
            "configYaml": str(org_dir / "config.dev.yaml"),
            "modules": [MODULE],
        },
    )
    plan = call(base_url, "/cdf/diff")
    assert any(kind["kind"] == "Space" for kind in plan["kinds"])
    deploy = call(base_url, "/cdf/deploy", {"dryRun": True})
    assert deploy["dryRun"] is True
