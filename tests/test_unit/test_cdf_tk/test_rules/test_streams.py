from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValueResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, InternalValidatorException
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.resource_ios import ResourceType, StreamIO
from cognite_toolkit._cdf_tk.rules._streams import StreamLimits, StreamRuleSet

API_URL = "https://bluefield.cognitedata.com/api/v1/projects/my_project"
NEW_STREAM = ExternalId(external_id="new-stream")


def _stream_resource(tmp_path: Path, external_id: str, template: str) -> BuiltResource:
    path = tmp_path / f"{external_id}.Streams.yaml"
    path.write_text(f"externalId: {external_id}\nsettings:\n  template:\n    name: {template}\n")
    return BuiltResource(
        identifier=ExternalId(external_id=external_id),
        source_hash="test-hash",
        type=ResourceType(resource_folder=StreamIO.folder_name, kind=StreamIO.kind),
        source_path=path.resolve(),
        build_path=path.resolve(),
        crud_cls=StreamIO,
        has_syntax_error=False,
        module_id=ModuleId(id=Path("modules/my"), path=tmp_path.resolve()),
    )


def _existing(*templates: str) -> list[StreamResponse]:
    return [
        StreamResponse(
            external_id=f"existing-{index}",
            created_time=0,
            created_from_template=template,
            type="Mutable",
        )
        for index, template in enumerate(templates)
    ]


def _limits(max_active: float = 3.0, **by_template: float) -> list[LimitValueResponse]:
    return [
        LimitValueResponse(limit_id="streams.max_active_streams", value=max_active),
        LimitValueResponse(limit_id="streams.max_soft_deleted_streams", value=30.0),
        *(
            LimitValueResponse(limit_id=f"streams.template.{template}.max_active_streams", value=value)
            for template, value in by_template.items()
        ),
    ]


def _client(limits: list[LimitValueResponse], existing: list[StreamResponse]) -> MagicMock:
    client = MagicMock()
    client.config.create_api_url.side_effect = lambda endpoint: f"{API_URL}{endpoint}"
    client.limits.list.return_value = limits
    client.streams.list.return_value = existing
    return client


def _deployment(created: list[ExternalId], deleted: list[ExternalId] | None = None) -> DeploymentResult:
    return DeploymentResult(
        resource_name="streams",
        is_dry_run=True,
        created_ids=list(created),
        updated_ids=[],
        deleted_ids=list(deleted or []),
        unchanged_ids=[],
        is_missing_write_acl=False,
    )


class TestStreamRuleSetStatus:
    @pytest.mark.parametrize(
        "has_client, has_results, expected",
        [
            pytest.param(False, True, "skip", id="no-client"),
            pytest.param(True, False, "skip", id="no-results"),
            pytest.param(True, True, "ready", id="client-and-results"),
        ],
    )
    def test_get_status(self, has_client: bool, has_results: bool, expected: str) -> None:
        rule = StreamRuleSet(
            resources=[],
            results=[_deployment([NEW_STREAM])] if has_results else None,
            client=MagicMock() if has_client else None,
        )
        assert rule.get_status().code == expected


class TestStreamLimits:
    def test_parses_total_and_template_limits(self) -> None:
        parsed = StreamLimits.from_responses(_limits(3.0, basic_archive=2.0, basic_live_data=1.0))

        assert (parsed, parsed.template_limit("BasicLiveData")) == (
            StreamLimits(
                max_active_streams=3,
                max_active_streams_by_template={"basic_archive": 2, "basic_live_data": 1},
            ),
            1,
        )


class TestStreamCapacity:
    @pytest.mark.parametrize(
        "limits, existing, deleted, expected_fragments",
        [
            pytest.param(
                _limits(2.0),
                _existing("BasicArchive", "BasicArchive"),
                [],
                ["increasing the number of active streams from 2 to 3, which exceeds the project limit of 2"],
                id="total-limit",
            ),
            pytest.param(
                _limits(10.0, basic_live_data=1.0),
                _existing("BasicLiveData"),
                [],
                ["the number of active 'BasicLiveData' streams from 1 to 2, which exceeds the project limit of 1"],
                id="template-limit",
            ),
            pytest.param(_limits(3.0, basic_live_data=1.0), _existing("BasicArchive"), [], [], id="room-left"),
            pytest.param(
                _limits(1.0, basic_live_data=1.0),
                _existing("BasicLiveData"),
                [ExternalId(external_id="existing-0")],
                [],
                id="deletion-frees-capacity",
            ),
        ],
    )
    def test_capacity(
        self,
        tmp_path: Path,
        limits: list[LimitValueResponse],
        existing: list[StreamResponse],
        deleted: list[ExternalId],
        expected_fragments: list[str],
    ) -> None:
        rule = StreamRuleSet(
            resources=[_stream_resource(tmp_path, NEW_STREAM.external_id, "BasicLiveData")],
            results=[_deployment([NEW_STREAM], deleted)],
            client=_client(limits, existing),
        )

        insights = [item for item in rule.validate() if isinstance(item, BuildError)]
        assert [
            (insight.code, insight.source_url, fragment in insight.message)
            for insight, fragment in zip(insights, expected_fragments, strict=True)
        ] == [("EXCEEDED-LIMIT", f"{API_URL}/streams", True)] * len(expected_fragments)

    def test_no_api_calls_without_created_streams(self) -> None:
        client = _client(_limits(), [])
        rule = StreamRuleSet(resources=[], results=[_deployment([])], client=client)

        assert (list(rule.validate()), client.limits.list.called, client.streams.list.called) == ([], False, False)

    def test_api_failure_is_reported(self) -> None:
        client = _client(_limits(), [])
        client.streams.list.side_effect = ToolkitAPIError("streams unavailable")
        rule = StreamRuleSet(resources=[], results=[_deployment([NEW_STREAM])], client=client)

        items = list(rule.validate())
        assert ([type(item) for item in items], "streams unavailable" in items[0].message) == (
            [InternalValidatorException],
            True,
        )
