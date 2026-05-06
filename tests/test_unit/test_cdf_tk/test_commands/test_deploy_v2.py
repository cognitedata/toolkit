from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, SpaceStatistics

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._container import (
    ContainerInspectResult,
    ContainerInspectResultItem,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._space import SpaceResponse
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentStep, DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, EdgeCRUD, NodeCRUD, SpaceCRUD, ViewIO


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
            lambda *args: panel_calls.append(args[2]),
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
            lambda soft_deleted, limit, count, console: None,
        )

        result = cmd._confirm_drop_data(client, [], options)

        assert result is False

    def test_raises_when_soft_delete_limit_exceeded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the capacity check fails, ToolkitValueError is raised before prompts."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        # Nearly full soft-delete limit: 9_200_000 used of 10_000_000 → adding 900_000 fails
        client = _make_client(soft_deleted=9_200_000, limit=10_000_000)
        options = DeployOptions(drop=True, drop_data=True)

        monkeypatch.setattr(cmd, "_count_dms_instances_in_plan", lambda c, p, o: 900_000)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.print_soft_delete_panel",
            lambda soft_deleted, limit, count, console: None,
        )

        with pytest.raises(ToolkitValueError, match="Cannot proceed with dropping data from resources"):
            cmd._confirm_drop_data(client, [], options)


class TestCountDmsInstancesInPlan:
    def _make_client(self, space_stats: SpaceStatistics | None = None) -> MagicMock:
        client = MagicMock()
        client.data_modeling.statistics.spaces.retrieve.return_value = [space_stats] if space_stats else None
        return client

    def test_returns_zero_for_empty_plan(self) -> None:
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        assert cmd._count_dms_instances_in_plan(MagicMock(), [], DeployOptions()) == 0

    def test_skips_non_instance_cruds(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """ContainerCRUD steps must not contribute to the count."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD

        monkeypatch.setattr(cmd, "_read_resource_files", lambda crud, files, opts: {"key": MagicMock()})
        plan = [DeploymentStep(crud_cls=ContainerCRUD, files=[])]
        assert cmd._count_dms_instances_in_plan(MagicMock(), plan, DeployOptions()) == 0

    def test_counts_nodes_and_edges_from_space_stats(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """For SpaceCRUD, sum nodes + edges from space-level statistics."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        space = SpaceResponse(space="my_space", is_global=False, created_time=0, last_updated_time=0)
        client = self._make_client(SpaceStatistics("my_space", 0, 0, 0, 40, 0, 50, 0))
        monkeypatch.setattr(cmd, "_read_resource_files", lambda crud, files, opts: {"my_space": MagicMock()})
        with patch.object(SpaceCRUD, "retrieve", return_value=[space]):
            plan = [DeploymentStep(crud_cls=SpaceCRUD, files=[])]
            assert cmd._count_dms_instances_in_plan(client, plan, DeployOptions()) == 90

    def test_counts_len_for_node_and_edge_cruds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """For NodeCRUD and EdgeCRUD, count is simply the number of existing resources."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        existing = [MagicMock(), MagicMock(), MagicMock()]
        monkeypatch.setattr(cmd, "_read_resource_files", lambda crud, files, opts: {"k": MagicMock()})
        with (
            patch.object(NodeCRUD, "retrieve", return_value=existing[:2]),
            patch.object(EdgeCRUD, "retrieve", return_value=existing[2:]),
        ):
            plan = [
                DeploymentStep(crud_cls=NodeCRUD, files=[]),
                DeploymentStep(crud_cls=EdgeCRUD, files=[]),
            ]
            assert cmd._count_dms_instances_in_plan(MagicMock(), plan, DeployOptions()) == 3

    def test_returns_zero_when_space_stats_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If space statistics are unavailable, that space contributes 0."""
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        space = SpaceResponse(space="my_space", is_global=False, created_time=0, last_updated_time=0)
        client = self._make_client(space_stats=None)
        monkeypatch.setattr(cmd, "_read_resource_files", lambda crud, files, opts: {"my_space": MagicMock()})
        with patch.object(SpaceCRUD, "retrieve", return_value=[space]):
            plan = [DeploymentStep(crud_cls=SpaceCRUD, files=[])]
            assert cmd._count_dms_instances_in_plan(client, plan, DeployOptions()) == 0


_REFERENCING_VIEW = ViewId(space="my_space", external_id="ReferencingView", version="v1")


class TestCheckNoOutOfScopeViewReferences:
    @pytest.mark.parametrize(
        "plan_view_ids, hidden_view_count, operation, should_raise, error_match",
        [
            pytest.param(
                [_REFERENCING_VIEW],
                0,
                "clean",
                False,
                "",
                id="passes_when_referencing_view_in_plan",
            ),
            pytest.param(
                [],
                0,
                "clean",
                True,
                "Cannot proceed with cleaning resources",
                id="blocks_when_referencing_view_missing_from_plan",
            ),
            pytest.param(
                [_REFERENCING_VIEW],
                1,
                "deploy",
                True,
                "Cannot proceed with deploying",
                id="blocks_when_hidden_views_exist",
            ),
        ],
    )
    def test_validate_plan_container_references(
        self,
        plan_view_ids: list[ViewId],
        hidden_view_count: int,
        operation: str,
        should_raise: bool,
        error_match: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cmd = DeployV2Command(print_warning=False, skip_tracking=True)
        container_mock = MagicMock()
        container_mock.space = "my_space"
        container_mock.external_id = "my_container"
        mock_container_crud = MagicMock()
        mock_container_crud.retrieve.return_value = [container_mock]
        mock_view_crud = MagicMock()

        def mock_read_files(crud, files, opts):
            if crud is mock_container_crud:
                return {"container_key": MagicMock()}
            return {view_id: MagicMock() for view_id in plan_view_ids}

        monkeypatch.setattr(cmd, "_read_resource_files", mock_read_files)
        inspect_result = ContainerInspectResultItem(
            space="my_space",
            external_id="my_container",
            inspection_results=ContainerInspectResult(
                involved_view_count=1 + hidden_view_count,
                involved_views=[_REFERENCING_VIEW],
            ),
        )
        client = MagicMock()
        client.tool.containers.inspect.return_value = [inspect_result]
        plan = [
            DeploymentStep(crud_cls=ContainerCRUD, files=[]),
            DeploymentStep(crud_cls=ViewIO, files=[]),
        ]
        raises_ctx = pytest.raises(ToolkitValueError, match=error_match) if should_raise else nullcontext()
        with (
            patch.object(ContainerCRUD, "create_loader", return_value=mock_container_crud),
            patch.object(ViewIO, "create_loader", return_value=mock_view_crud),
            raises_ctx,
        ):
            cmd._validate_plan_container_references(client, plan, DeployOptions(drop=True, operation=operation))
