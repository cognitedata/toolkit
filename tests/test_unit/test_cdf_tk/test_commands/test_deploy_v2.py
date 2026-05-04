from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics

from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def _make_instance_statistics(soft_deleted: int = 300, limit: int = 10_000_000) -> InstanceStatistics:
    return InstanceStatistics(
        nodes=10_000,
        edges=5_000,
        soft_deleted_nodes=100,
        soft_deleted_edges=200,
        instances_limit=limit,
        instances=15_000,
        soft_deleted_instances=soft_deleted,
        soft_deleted_instances_limit=limit,
    )


def _make_client(project: str = "my-project", soft_deleted: int = 300, limit: int = 10_000_000) -> MagicMock:
    client = MagicMock()
    client.config.project = project
    client.console = MagicMock()
    stats = MagicMock()
    stats.instances = _make_instance_statistics(soft_deleted, limit)
    client.data_modeling.statistics.project.return_value = stats
    return client


class TestConfirmDropData:
    def test_returns_true_no_instances(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When no DMS instances would be soft-deleted, skip the soft-delete panel and
        go straight to the warning panel + project name confirmation."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        client = _make_client()
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 0)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.confirm_by_typing_project_name",
            lambda msg, client: True,
        )

        result = cmd._confirm_drop_data(client, [], options)
        assert result is True
        # The project-level statistics should not have been fetched (no instances)
        client.data_modeling.statistics.project.assert_not_called()

    def test_returns_false_when_user_declines_project_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the user declines the project name confirmation, return False."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        client = _make_client()
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 0)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.confirm_by_typing_project_name",
            lambda msg, client: False,
        )

        result = cmd._confirm_drop_data(client, [], options)
        assert result is False

    def test_soft_delete_panel_shown_when_instances_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When DMS instances exist, the soft-delete panel + confirm prompt are shown."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        client = _make_client()
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 5_000)
        mock_questionary = MagicMock()
        mock_questionary.confirm.return_value.ask.return_value = True
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.deploy_v2.command.questionary", mock_questionary)
        panel_calls: list[Any] = []
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.print_soft_delete_panel",
            lambda stats, count: panel_calls.append(count),
        )
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.confirm_by_typing_project_name",
            lambda msg, client: True,
        )

        result = cmd._confirm_drop_data(client, [], options)

        assert result is True
        assert panel_calls == [5_000]
        mock_questionary.confirm.assert_called_once()

    def test_returns_false_when_user_declines_soft_delete_prompt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the user answers N to the soft-delete acknowledge prompt, return False."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        client = _make_client()
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 5_000)
        mock_questionary = MagicMock()
        mock_questionary.confirm.return_value.ask.return_value = False
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.deploy_v2.command.questionary", mock_questionary)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.print_soft_delete_panel",
            lambda stats, count: None,
        )

        result = cmd._confirm_drop_data(client, [], options)

        assert result is False

    def test_raises_when_soft_delete_limit_exceeded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the headroom check fails, ToolkitValueError is raised before prompts."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        # Nearly full soft-delete limit: 9_200_000 used of 10_000_000 → adding 900_000 fails
        client = _make_client(soft_deleted=9_200_000, limit=10_000_000)
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 900_000)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.print_soft_delete_panel",
            lambda stats, count: None,
        )

        with pytest.raises(ToolkitValueError, match="Cannot proceed with dropping data from resources"):
            cmd._confirm_drop_data(client, [], options)
