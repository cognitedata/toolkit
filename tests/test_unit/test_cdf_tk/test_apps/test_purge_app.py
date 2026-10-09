from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.apps._purge import PurgeApp
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceAggregateResponse,
    InstanceAggregateResult,
    InstanceAggregateValue,
    SpaceResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import SpaceStatisticsResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._purge import PurgeCommand
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceSelector
from tests.test_unit.utils import MockQuestionary


def _instance_count(value: float) -> InstanceAggregateResponse:
    return InstanceAggregateResponse(
        items=[
            InstanceAggregateResult(
                instance_type="node",
                aggregates=[InstanceAggregateValue(aggregate="count", property="externalId", value=value)],
            )
        ]
    )


def _space(space: str) -> SpaceResponse:
    return SpaceResponse(
        space=space,
        name=space,
        description=None,
        created_time=1,
        last_updated_time=1,
        is_global=False,
    )


def _space_stats(space: str, *, views: int = 0, nodes: int = 0) -> SpaceStatisticsResponse:
    return SpaceStatisticsResponse(
        space=space,
        containers=0,
        views=views,
        data_models=0,
        edges=0,
        soft_deleted_edges=0,
        nodes=nodes,
        soft_deleted_nodes=0,
    )


class TestPurgeInstancesInstanceSpace:
    def test_interactive_view_keeps_cli_instance_space(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`--instance-space` must limit the purge when the view is chosen interactively.

        The reported command is `cdf data purge instances --instance-space=SPACE_X` with no view.
        That opens an interactive view picker. If the chosen view's only instances live in another
        space, that space is auto-selected and purged. The CLI space must still be the only target.
        """
        cli_space = "SPACE_X"
        other_space = "SPACE_Y"
        schema_space = _space("schema_space")
        view = ViewResponse(
            space=schema_space.space,
            external_id="Asset",
            version="v1",
            properties={},
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            queryable=True,
            used_for="node",
            is_global=False,
            mapped_containers=[],
        )
        captured: list[InstanceSelector] = []

        def capture_instances(self: PurgeCommand, *, selector: InstanceSelector, **_: object) -> None:
            captured.append(selector)

        def aggregate_by_space(*_: object, **kwargs: object) -> InstanceAggregateResponse:
            filter_ = kwargs["filter"]
            space = filter_["equals"]["value"] if isinstance(filter_, dict) else None
            return _instance_count(10 if space == other_space else 0)

        with monkeypatch_toolkit_client() as client:
            client.tool.token.verify_acls.return_value = []
            client.tool.spaces.list.return_value = [schema_space, _space(cli_space), _space(other_space)]
            client.tool.views.list.return_value = [view]
            client.tool.instances.aggregate.side_effect = aggregate_by_space
            client.statistics.spaces.list.return_value = [
                _space_stats(schema_space.space, views=1),
                _space_stats(cli_space, nodes=1),
                _space_stats(other_space, nodes=10),
            ]
            client.statistics.retrieve.return_value = MagicMock(concurrent_read_limit=4)

            environment = MagicMock()
            environment.get_client.return_value = client
            monkeypatch.setattr(
                "cognite_toolkit._cdf_tk.apps._purge.EnvironmentVariables.create_from_environment",
                lambda: environment,
            )
            monkeypatch.setattr(PurgeCommand, "instances", capture_instances)

            with MockQuestionary(
                [
                    "cognite_toolkit._cdf_tk.utils.interactive_select",
                    "cognite_toolkit._cdf_tk.apps._purge",
                ],
                monkeypatch,
                [schema_space, view, True, True],
            ):
                PurgeApp.purge_instances(view=None, instance_space=[cli_space])

        assert [selector.get_instance_spaces() for selector in captured] == [[cli_space]]
