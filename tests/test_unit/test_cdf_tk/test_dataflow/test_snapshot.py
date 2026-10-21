import json
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.dataflow import DataFlowNode, FlowKind, Snapshot, SnapshotEdge
from cognite_toolkit._cdf_tk.dataflow._io import dump_json, load_json, load_snapshot, save_snapshot
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def node(node_id: str) -> DataFlowNode:
    return DataFlowNode(id=node_id, type="t/Kind", label=node_id)


@pytest.fixture
def snapshot() -> Snapshot:
    return Snapshot.create(
        [node("c"), node("b"), node("a")],
        [
            SnapshotEdge(source="a", target="b", kind=FlowKind.READS),
            SnapshotEdge(source="b", target="c", kind=FlowKind.WRITES),
            SnapshotEdge(source="a", target="b", kind=FlowKind.READS),
        ],
        generated_at="2026-01-01T00:00:00+00:00",
        cdf_project="p",
    )


def test_round_trip(snapshot: Snapshot, tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    assert save_snapshot(snapshot, path)

    assert load_snapshot(path) == snapshot
    assert [n.id for n in snapshot.nodes] == ["a", "b", "c"]
    assert len(snapshot.edges) == 2


def test_save_skips_unchanged_content(snapshot: Snapshot, tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    save_snapshot(snapshot, path)
    later = snapshot.model_copy(update={"generated_at": "2027-01-01T00:00:00+00:00"})

    assert not save_snapshot(later, path)
    assert load_snapshot(path).generated_at == snapshot.generated_at

    changed = Snapshot.create([node("a")], [], generated_at="2027-01-01T00:00:00+00:00")
    assert save_snapshot(changed, path)


def test_unknown_schema_version_fails_clearly(snapshot: Snapshot) -> None:
    data = json.loads(dump_json(snapshot))
    data["schemaVersion"] = 99

    with pytest.raises(ToolkitValueError, match="schema version 99"):
        load_json(json.dumps(data))


def test_neighbors_filters_direction_depth_and_kind(snapshot: Snapshot) -> None:
    out_one = snapshot.neighbors("a", "out", depth=1)
    assert {n.id for n in out_one.nodes} == {"a", "b"}

    out_two = snapshot.neighbors("a", "out", depth=2)
    assert {n.id for n in out_two.nodes} == {"a", "b", "c"}

    assert {n.id for n in snapshot.neighbors("c", "in", depth=1).nodes} == {"b", "c"}
    assert {n.id for n in snapshot.neighbors("b", "both", depth=1, kinds={FlowKind.WRITES}).nodes} == {"b", "c"}


def test_neighbors_limit_keeps_nearest_nodes_and_no_dangling_edges(snapshot: Snapshot) -> None:
    limited = snapshot.neighbors("a", "out", depth=2, limit=2)

    assert limited.truncated
    assert limited.total_nodes == 3
    assert {n.id for n in limited.nodes} == {"a", "b"}
    assert [(e.source, e.target) for e in limited.edges] == [("a", "b")]
    assert not snapshot.neighbors("a", "out", depth=2, limit=3).truncated
    assert {n.id for n in snapshot.neighbors("a", "out", limit=0).nodes} == {"a"}


def test_search_filters_pages_and_counts() -> None:
    snapshot = Snapshot.create(
        [
            DataFlowNode(id="1", type="data_modeling/View", label="Pump", folder="data_modeling", kind="View"),
            DataFlowNode(id="2", type="data_modeling/View", label="Big pump", folder="data_modeling", kind="View"),
            DataFlowNode(
                id="3",
                type="transformations/Transformation",
                label="tr_pump",
                folder="transformations",
                kind="Transformation",
            ),
            DataFlowNode(id="4", type="collection", label="_cdf.assets"),
        ],
        [],
    )

    page = snapshot.search("pump", limit=2)
    assert page.total == 3
    assert [n.id for n in page.nodes] == ["1", "2"]
    assert page.counts == {"data_modeling/View": 2, "transformations/Transformation": 1}
    assert [n.id for n in snapshot.search("pump", limit=2, offset=2).nodes] == ["3"]
    assert [n.id for n in snapshot.search(kind="view").nodes] == ["2", "1"]
    assert [n.id for n in snapshot.search(node_type="collection").nodes] == ["4"]


@pytest.fixture
def pipeline() -> Snapshot:
    return Snapshot.create(
        [node(name) for name in ("raw", "tr1", "view1", "tr2", "view2", "wf")],
        [
            SnapshotEdge(source="tr1", target="raw", kind=FlowKind.READS),
            SnapshotEdge(source="tr1", target="view1", kind=FlowKind.WRITES),
            SnapshotEdge(source="tr2", target="view1", kind=FlowKind.READS),
            SnapshotEdge(source="tr2", target="view2", kind=FlowKind.WRITES),
            SnapshotEdge(source="wf", target="tr1", kind=FlowKind.CONTAINS),
        ],
    )


def test_lineage_resolves_upstream_downstream_and_orchestration(pipeline: Snapshot) -> None:
    lineage = pipeline.lineage("view1")

    assert [(s.id, s.distance, s.role) for s in lineage.upstream] == [("tr1", 1, "process"), ("raw", 2, "data")]
    assert [(s.id, s.distance, s.role) for s in lineage.downstream] == [("tr2", 1, "process"), ("view2", 2, "data")]
    assert lineage.orchestration == {"tr1": ["wf"]}
    assert {n.id for n in lineage.nodes} == {"raw", "tr1", "view1", "tr2", "view2", "wf"}
    assert not lineage.truncated

    only = pipeline.lineage("view1", only_processes=True, orchestration=False)
    assert [s.id for s in only.upstream + only.downstream] == ["tr1", "tr2"]
    assert {n.id for n in only.nodes} == {"view1", "tr1", "tr2"}
    assert only.orchestration == {}

    assert [s.id for s in pipeline.lineage("view1", "upstream", depth=1).upstream] == ["tr1"]
    assert pipeline.lineage("view1", "upstream").downstream == []


def test_lineage_terminates_on_cycle_and_flags_incomplete(pipeline: Snapshot) -> None:
    cyclic = Snapshot.create(
        [*pipeline.nodes, DataFlowNode(id="u", type="unresolved", label="u", stub=True)],
        [
            *pipeline.edges,
            SnapshotEdge(source="tr1", target="view2", kind=FlowKind.READS),
            SnapshotEdge(source="tr1", target="u", kind=FlowKind.READS),
        ],
    )

    lineage = cyclic.lineage("view1")

    assert {s.id for s in lineage.upstream} == {"tr1", "raw", "view2", "tr2", "u"}
    assert lineage.incomplete
    assert not pipeline.lineage("view1").incomplete
    assert pipeline.lineage("view1", limit=2).truncated
