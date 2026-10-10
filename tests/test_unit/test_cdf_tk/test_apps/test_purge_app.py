from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.apps._purge import PurgeApp
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._purge import PurgeCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from tests.test_unit.utils import MockQuestionary


def _mock_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch_toolkit_client() as client:
        client.tool.token.verify_acls.return_value = []
        environment = MagicMock()
        environment.get_client.return_value = client
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._purge.EnvironmentVariables.create_from_environment",
            lambda: environment,
        )


class TestPurgeInteractiveOptions:
    def test_interactive_instances_rejects_ignored_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_instances(
                view=None,
                instance_space=["SPACE_X"],
                dry_run=True,
                unlink=False,
                verbose=True,
            )

        assert str(exc_info.value) == (
            "Cannot specify --instance-space, --dry-run, --skip-unlink, --verbose when running in interactive mode. "
            "Please omit these options."
        )

    def test_interactive_dataset_rejects_ignored_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_dataset(MagicMock(), external_id=None, skip_data=True, asset_recursive=True)

        assert (
            str(exc_info.value) == "Cannot specify --skip-data, --asset-recursive when running in interactive mode. "
            "Please omit these options."
        )

    def test_interactive_space_rejects_ignored_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_space(
                MagicMock(),
                space=None,
                include_space=True,
                delete_datapoints=True,
                delete_file_content=True,
                dry_run=True,
                verbose=True,
            )

        assert str(exc_info.value) == (
            "Cannot specify --include-space, --delete-datapoints, --delete-file-content, --dry-run, --verbose "
            "when running in interactive mode. Please omit these options."
        )

    def test_interactive_space_prompts_for_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def capture_space(self: PurgeCommand, **kwargs: Any) -> None:
            captured.update(kwargs)

        class _Select:
            def __init__(self, *_: Any, **__: Any) -> None:
                return None

            def select_space_type(self) -> str:
                return "instance"

            def select_instance_space(self, multiselect: bool = False) -> str:
                return "SPACE_X"

        _mock_environment(monkeypatch)
        monkeypatch.setattr("cognite_toolkit._cdf_tk.apps._purge.DataModelingSelect", _Select)
        monkeypatch.setattr(PurgeCommand, "space", capture_space)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.apps._purge",
            monkeypatch,
            [False, True, True, False, True],
        ):
            PurgeApp.purge_space(MagicMock(), space=None)

        assert {
            "selected_space": captured["selected_space"],
            "dry_run": captured["dry_run"],
            "include_space": captured["include_space"],
            "delete_datapoints": captured["delete_datapoints"],
            "delete_file_content": captured["delete_file_content"],
            "verbose": captured["verbose"],
        } == {
            "selected_space": "SPACE_X",
            "dry_run": False,
            "include_space": True,
            "delete_datapoints": True,
            "delete_file_content": False,
            "verbose": True,
        }

    def test_interactive_instances_prompts_for_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def capture_instances(self: PurgeCommand, **kwargs: Any) -> None:
            captured.update(kwargs)

        class _View:
            space = "schema_space"
            external_id = "Asset"
            version = "v1"
            used_for = "node"

            def as_id(self) -> "_View":
                return self

        class _Select:
            def __init__(self, *_: Any, **__: Any) -> None:
                return None

            def select_view(self, filter: Any = None) -> _View:
                return _View()

            def select_instance_type(self, view_used_for: str) -> str:
                return "edge"

            def select_instance_space(self, multiselect: bool, view_id: _View, instance_type: str) -> list[str]:
                return ["SPACE_X"]

        _mock_environment(monkeypatch)
        monkeypatch.setattr("cognite_toolkit._cdf_tk.apps._purge.DataModelingSelect", _Select)
        monkeypatch.setattr(PurgeCommand, "instances", capture_instances)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.apps._purge",
            monkeypatch,
            [False, False, True],
        ):
            PurgeApp.purge_instances(view=None)

        assert {
            "dry_run": captured["dry_run"],
            "unlink": captured["unlink"],
            "verbose": captured["verbose"],
            "instance_spaces": captured["selector"].instance_spaces,
            "instance_type": captured["selector"].instance_type,
        } == {
            "dry_run": False,
            "unlink": False,
            "verbose": True,
            "instance_spaces": ("SPACE_X",),
            "instance_type": "edge",
        }
