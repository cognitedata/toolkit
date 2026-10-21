from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RawTableId
from cognite_toolkit._cdf_tk.dataflow import DataFlowEdge, FlowKind, FlowTarget, resource_node_id
from cognite_toolkit._cdf_tk.dataflow import _collector as collector_module
from cognite_toolkit._cdf_tk.dataflow._collector import CollectScope, collect
from cognite_toolkit._cdf_tk.resource_ios import RawTableIO, SpaceIO


class FakeResponse(BaseModel):
    external_id: str
    last_updated_time: int | None = None
    id: int = 1
    fail_dump: bool = False
    table: str = "t"

    def dump(self) -> dict[str, Any]:
        return self.model_dump()


class FakeYaml(BaseModel):
    external_id: str
    table: str = "t"


def make_io(name: str, responses: list[FakeResponse], flow: bool) -> Any:
    """A stand-in for a ResourceIO. The collector only needs this small part of the interface."""

    class FakeIO:
        folder_name = "fake"
        kind = name
        dump_calls: ClassVar[list[str]] = []
        listing_error: Exception | None = None

        @classmethod
        def create_io(cls, client: Any) -> "FakeIO":
            return cls()

        def iterate(self, **_: Any) -> Iterable[FakeResponse]:
            if self.listing_error is not None:
                raise self.listing_error
            return list(responses)

        def dump_resource(self, response: FakeResponse) -> dict[str, Any]:
            self.dump_calls.append(response.external_id)
            if response.fail_dump:
                raise RuntimeError("cannot dump")
            return {"external_id": response.external_id, "table": response.table}

        @classmethod
        def get_id(cls, item: FakeResponse) -> ExternalId:
            return ExternalId(external_id=item.external_id)

        @classmethod
        def validate_object(cls, resource: dict[str, Any], extra: str = "ignore") -> FakeYaml:
            return FakeYaml.model_validate(resource)

        @classmethod
        def get_dependencies(cls, resource: FakeYaml) -> Iterable[tuple[Any, ExternalId]]:
            yield SpaceIO, ExternalId(external_id="space-1")

        @classmethod
        def get_data_flow(cls, resource: FakeYaml) -> Iterable[DataFlowEdge]:
            if flow:
                yield DataFlowEdge(
                    source=resource_node_id(cls, ExternalId(external_id=resource.external_id)),  # ty: ignore[invalid-argument-type]
                    target=FlowTarget.resource(RawTableIO, RawTableId(db_name="db", name=resource.table)),
                    kind=FlowKind.READS,
                )

    return FakeIO


@pytest.fixture
def client() -> Any:
    return SimpleNamespace(config=SimpleNamespace(project="my-project"))


def run(
    client: Any,
    flow_io: Any,
    structure: list[Any] | None = None,
    previous: Any = None,
    monkeypatch: pytest.MonkeyPatch | None = None,
    scope: CollectScope | None = None,
):
    assert monkeypatch is not None
    monkeypatch.setattr(collector_module, "get_flow_classes", lambda: [flow_io])
    return collect(client, previous=previous, scope=scope, structure_classes=structure or [])


def test_flow_and_requires_edges_and_unreferenced_nodes(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=1)], flow=True)
    structure_io = make_io("Plain", [FakeResponse(external_id="unreferenced", last_updated_time=1)], flow=False)

    snapshot = run(client, flow_io, [structure_io], monkeypatch=monkeypatch)

    nodes = snapshot.node_by_id()
    assert {node.label for node in nodes.values() if not node.stub} == {"a", "unreferenced"}
    assert {(edge.kind, nodes[edge.target].label) for edge in snapshot.edges if "Flow" in edge.source} == {
        (FlowKind.READS, "db.t"),
        (FlowKind.REQUIRES, "space-1"),
    }
    assert all(node.stub for node in nodes.values() if node.label in {"db.t", "space-1"})
    assert snapshot.cdf_project == "my-project"


def test_one_failing_resource_does_not_abort(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io(
        "Flow",
        [
            FakeResponse(external_id="bad", last_updated_time=1, fail_dump=True),
            FakeResponse(external_id="ok", last_updated_time=1),
        ],
        flow=True,
    )

    snapshot = run(client, flow_io, monkeypatch=monkeypatch)

    by_label = {node.label: node for node in snapshot.nodes if not node.stub}
    assert by_label["bad"].status == "failed"
    assert "cannot dump" in (by_label["bad"].error or "")
    assert by_label["ok"].status == "ok"
    assert any(edge.source == by_label["ok"].id for edge in snapshot.edges)


def test_unchanged_version_token_reuses_previous_edges(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=5)], flow=True)
    first = run(client, flow_io, monkeypatch=monkeypatch)
    flow_io.dump_calls.clear()

    second = run(client, flow_io, previous=first, monkeypatch=monkeypatch)

    assert flow_io.dump_calls == []
    assert second.content_hash() == first.content_hash()

    flow_io_changed = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=6, table="other")], flow=True)
    third = run(client, flow_io_changed, previous=first, monkeypatch=monkeypatch)

    assert flow_io_changed.dump_calls == ["a"]
    assert "db.other" in {node.label for node in third.nodes}


def test_content_hash_token_when_no_last_updated_time(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io("Flow", [FakeResponse(external_id="a")], flow=True)
    first = run(client, flow_io, monkeypatch=monkeypatch)
    flow_io.dump_calls.clear()

    run(client, flow_io, previous=first, monkeypatch=monkeypatch)

    assert flow_io.dump_calls == []
    assert first.node_by_id()[resource_node_id(flow_io, ExternalId(external_id="a"))].version_token.startswith("h")  # type: ignore[union-attr]


def test_deleted_resource_disappears_and_edge_target_becomes_dangling(
    client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    both = make_io(
        "Flow",
        [FakeResponse(external_id="a", last_updated_time=1), FakeResponse(external_id="b", last_updated_time=1)],
        flow=True,
    )
    first = run(client, both, monkeypatch=monkeypatch)

    only_a = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=1)], flow=True)
    second = run(client, only_a, previous=first, monkeypatch=monkeypatch)

    assert {node.label for node in second.nodes if not node.stub} == {"a"}
    assert not any(edge.source == resource_node_id(only_a, ExternalId(external_id="b")) for edge in second.edges)


def test_failed_listing_keeps_previous_nodes(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=1)], flow=True)
    first = run(client, flow_io, monkeypatch=monkeypatch)

    flow_io.listing_error = PermissionError("forbidden")
    second = run(client, flow_io, previous=first, monkeypatch=monkeypatch)

    assert {node.label for node in second.nodes if not node.stub} == {"a"}
    assert second.errors == ["fake/Flow: PermissionError: forbidden"]


def test_system_space_resources_are_not_listed(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_io = make_io("Flow", [FakeResponse(external_id="a", last_updated_time=1)], flow=True)
    flow_io.get_id = classmethod(lambda cls, item: SpaceIdLike(space="cdf_cdm"))  # type: ignore[method-assign]

    snapshot = run(client, flow_io, monkeypatch=monkeypatch)

    assert [node for node in snapshot.nodes if not node.stub] == []


class SpaceIdLike:
    def __init__(self, space: str) -> None:
        self.space = space

    def dump(self) -> dict[str, str]:
        return {"space": self.space}
