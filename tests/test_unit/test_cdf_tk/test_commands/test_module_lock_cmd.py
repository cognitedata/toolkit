from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock, monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import ModuleLockCommand
from cognite_toolkit._cdf_tk.commands.deploy_v2.module_lock import SPACE, VIEW_ID
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from tests.test_unit.utils import MockQuestionary

REPO = "github.com/cognitedata/toolkit"


def _view(view_id: ViewId) -> MagicMock:
    view = MagicMock()
    view.as_id.return_value = view_id
    return view


def _lock_node(
    module_id: str, *, lock: bool = True, age: timedelta = timedelta(hours=1), view_id: ViewId = VIEW_ID
) -> NodeResponse:
    properties: dict[str, object] = {
        "moduleId": module_id,
        "timestamp": (datetime.now(UTC) - age).isoformat(),
        "gitName": "bob",
        "userId": "user-456",
        "branch": "feature/x",
        "lock": lock,
    }
    if view_id == VIEW_ID:
        properties["repo"] = REPO
    return NodeResponse.model_validate(
        {
            "instanceType": "node",
            "space": SPACE,
            "externalId": module_id,
            "version": 3,
            "createdTime": 0,
            "lastUpdatedTime": 0,
            "properties": {SPACE: {f"{view_id.external_id}/{view_id.version}": properties}},
        }
    )


@pytest.fixture(autouse=True)
def wide_console(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid that rich wraps the table cells, which makes the output hard to assert on."""
    monkeypatch.setenv("COLUMNS", "300")


@pytest.fixture
def client() -> Iterator[ToolkitClientMock]:
    with monkeypatch_toolkit_client() as client:
        client.lookup.users = MagicMock()
        client.lookup.users.email.return_value = "bob@example.com"
        client.views.retrieve.return_value = [_view(VIEW_ID)]
        client.instances.list.return_value = []
        client.instances.create.return_value = []
        yield client


class TestModuleLockCommand:
    def test_no_view_deployed(self, client: ToolkitClientMock, capsys: pytest.CaptureFixture[str]) -> None:
        client.views.retrieve.return_value = []

        released = ModuleLockCommand(skip_tracking=True).locks(client)

        assert released == []
        assert "not been set up" in capsys.readouterr().out
        client.instances.list.assert_not_called()

    def test_space_does_not_exist(self, client: ToolkitClientMock, capsys: pytest.CaptureFixture[str]) -> None:
        client.views.retrieve.side_effect = ToolkitAPIError("Space does not exist", code=400)

        assert ModuleLockCommand(skip_tracking=True).locks(client) == []
        assert "not been set up" in capsys.readouterr().out

    def test_view_lookup_server_error_raises(self, client: ToolkitClientMock) -> None:
        client.views.retrieve.side_effect = ToolkitAPIError("Internal error", code=500)

        with pytest.raises(ToolkitValueError, match="500"):
            ModuleLockCommand(skip_tracking=True).locks(client)

    def test_no_locks(self, client: ToolkitClientMock, capsys: pytest.CaptureFixture[str]) -> None:
        assert ModuleLockCommand(skip_tracking=True).locks(client) == []
        assert "No module locks found" in capsys.readouterr().out

    def test_list_only(self, client: ToolkitClientMock, capsys: pytest.CaptureFixture[str]) -> None:
        client.instances.list.return_value = [
            _lock_node("active"),
            _lock_node("released", lock=False),
            _lock_node("expired", age=timedelta(days=3)),
        ]

        released = ModuleLockCommand(skip_tracking=True).locks(client, unlock=False)

        assert released == []
        out = capsys.readouterr().out
        assert "1 active" in out and "2 inactive" in out
        assert "bob@example.com" in out
        assert REPO in out
        client.instances.create.assert_not_called()

    def test_email_lookup_fails_falls_back_to_user_id(
        self, client: ToolkitClientMock, capsys: pytest.CaptureFixture[str]
    ) -> None:
        client.instances.list.return_value = [_lock_node("active")]
        client.lookup.users.email.side_effect = ToolkitAPIError("Forbidden", code=403)

        ModuleLockCommand(skip_tracking=True).locks(client, unlock=False)

        assert "user-456" in capsys.readouterr().out

    def test_release_selected_lock(self, client: ToolkitClientMock) -> None:
        client.instances.list.return_value = [_lock_node("a"), _lock_node("b")]

        def answer(choices: list) -> list:
            return [choices[0].value]

        with MockQuestionary(ModuleLockCommand.__module__, [answer, True]):
            released = ModuleLockCommand(skip_tracking=True).locks(client)

        assert [lock.module_id for lock in released] == ["a"]
        nodes = client.instances.create.call_args.args[0]
        assert [node.external_id for node in nodes] == ["a"]
        assert nodes[0].existing_version == 3
        source = nodes[0].sources[0]
        assert source.source == VIEW_ID
        assert source.properties["lock"] is False
        assert source.properties["repo"] == REPO

    def test_release_with_legacy_view(self, client: ToolkitClientMock) -> None:
        legacy = LEGACY_VIEW_IDS[0]
        client.views.retrieve.return_value = [_view(legacy)]
        client.instances.list.return_value = [_lock_node("a", view_id=legacy)]

        with MockQuestionary(ModuleLockCommand.__module__, [lambda choices: [choices[0].value], True]):
            released = ModuleLockCommand(skip_tracking=True).locks(client)

        assert [lock.module_id for lock in released] == ["a"]
        source = client.instances.create.call_args.args[0][0].sources[0]
        assert source.source == legacy
        assert "repo" not in source.properties

    def test_dry_run_does_not_release(self, client: ToolkitClientMock) -> None:
        client.instances.list.return_value = [_lock_node("a")]

        with MockQuestionary(ModuleLockCommand.__module__, [lambda choices: [choices[0].value]]):
            released = ModuleLockCommand(skip_tracking=True).locks(client, dry_run=True)

        assert [lock.module_id for lock in released] == ["a"]
        client.instances.create.assert_not_called()
