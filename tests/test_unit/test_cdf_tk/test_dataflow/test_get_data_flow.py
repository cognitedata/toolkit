from typing import Any

import pytest

from cognite_toolkit._cdf_tk.dataflow import DataFlowEdge, FlowKind, TargetType
from cognite_toolkit._cdf_tk.dataflow._dms_query import ViewReference, find_view_references
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    ExtractionPipelineIO,
    FunctionScheduleIO,
    HostedExtractorJobIO,
    ResourceIO,
    TransformationIO,
    WorkflowTriggerIO,
    WorkflowVersionIO,
)


def _edges(crud_cls: type[ResourceIO], resource: dict[str, Any]) -> list[DataFlowEdge]:
    return list(crud_cls.get_data_flow(crud_cls.validate_object(resource, extra="ignore")))


def _summary(edges: list[DataFlowEdge]) -> set[tuple[str, str]]:
    return {(edge.kind.value, edge.target.label) for edge in edges}


@pytest.fixture
def enable_data_products(monkeypatch: pytest.MonkeyPatch) -> None:
    original = FeatureFlag.is_enabled
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.DATA_PRODUCTS or original(flag))


def test_default_yields_nothing() -> None:
    assert not list(ResourceIO.get_data_flow(object()))  # type: ignore[arg-type]


class TestTransformation:
    def test_reads_and_writes(self) -> None:
        pytest.importorskip("sqlglot")
        edges = _edges(
            TransformationIO,
            {
                "externalId": "tr",
                "name": "tr",
                "ignoreNullFields": True,
                "query": "select * from `db`.`t` join _cdf.events e on true",
                "destination": {"type": "raw", "database": "out_db", "table": "out_t"},
            },
        )

        assert _summary(edges) == {
            ("reads", "db.t"),
            ("reads", "_cdf.events"),
            ("writes", "out_db.out_t"),
        }

    def test_asset_centric_destination_writes_to_collection(self) -> None:
        edges = _edges(
            TransformationIO,
            {"externalId": "tr", "name": "tr", "ignoreNullFields": True, "destination": {"type": "asset_hierarchy"}},
        )

        assert [(edge.kind, edge.target.type, edge.target.name) for edge in edges] == [
            (FlowKind.WRITES, TargetType.COLLECTION, "_cdf.assets")
        ]

    def test_placeholder_table_is_unresolved(self) -> None:
        pytest.importorskip("sqlglot")
        edges = _edges(
            TransformationIO,
            {
                "externalId": "tr",
                "name": "tr",
                "ignoreNullFields": True,
                "query": "select * from {{ db }}.t",
                "destination": {"type": "assets"},
            },
        )

        reads = [edge for edge in edges if edge.kind is FlowKind.READS]
        assert [edge.target.type for edge in reads] == [TargetType.UNRESOLVED]
        assert reads[0].target.fragment == "{{ db }}.t"

    def test_data_model_destination_and_view_source(self) -> None:
        pytest.importorskip("sqlglot")
        edges = _edges(
            TransformationIO,
            {
                "externalId": "tr",
                "name": "tr",
                "ignoreNullFields": True,
                "query": "select * from cdf_nodes('sp', 'Source', 'v1')",
                "destination": {
                    "type": "instances",
                    "dataModel": {"space": "sp", "externalId": "Model", "version": "v2", "destinationType": "Target"},
                },
            },
        )

        assert _summary(edges) == {("reads", "sp:Source(vv1)"), ("writes", "sp:Model(vv2)")}


def test_extraction_pipeline_writes_to_raw_tables() -> None:
    edges = _edges(
        ExtractionPipelineIO,
        {
            "externalId": "ep",
            "name": "ep",
            "dataSetExternalId": "ds",
            "rawTables": [{"dbName": "db", "tableName": "t"}],
        },
    )

    assert _summary(edges) == {("writes", "db.t")}


def test_function_schedule_triggers_function() -> None:
    edges = _edges(
        FunctionScheduleIO,
        {
            "name": "daily",
            "functionExternalId": "fn",
            "cronExpression": "0 8 * * *",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
    )

    assert _summary(edges) == {("triggers", "fn")}


def test_hosted_extractor_job_edges_include_the_mapping() -> None:
    edges = _edges(
        HostedExtractorJobIO,
        {
            "externalId": "job",
            "sourceId": "src",
            "destinationId": "dest",
            "format": {"type": "custom", "mappingId": "map"},
        },
    )

    assert _summary(edges) == {("reads", "src"), ("writes", "dest"), ("reads", "map")}


class TestWorkflowVersion:
    def test_contains_tasks(self) -> None:
        edges = _edges(
            WorkflowVersionIO,
            {
                "workflowExternalId": "wf",
                "version": "1",
                "workflowDefinition": {
                    "tasks": [
                        {"externalId": "a", "type": "function", "parameters": {"function": {"externalId": "fn"}}},
                        {
                            "externalId": "b",
                            "type": "subworkflow",
                            "parameters": {
                                "subworkflow": {
                                    "tasks": [
                                        {
                                            "externalId": "c",
                                            "type": "transformation",
                                            "parameters": {"transformation": {"externalId": "tr"}},
                                        }
                                    ]
                                }
                            },
                        },
                    ]
                },
            },
        )

        assert _summary(edges) == {("contains", "fn"), ("contains", "tr")}

    def test_contains_edges_carry_task_order(self) -> None:
        def task(external_id: str, transformation: str, *depends_on: str) -> dict:
            return {
                "externalId": external_id,
                "type": "transformation",
                "parameters": {"transformation": {"externalId": transformation}},
                "dependsOn": [{"externalId": dep} for dep in depends_on],
            }

        edges = _edges(
            WorkflowVersionIO,
            {
                "workflowExternalId": "wf",
                "version": "1",
                "workflowDefinition": {
                    "tasks": [
                        task("a", "tr_a"),
                        {
                            "externalId": "gate",
                            "type": "dynamic",
                            "parameters": {"dynamic": {"tasks": "${a.output}"}},
                            "dependsOn": [{"externalId": "a"}],
                        },
                        task("b", "tr_b", "gate"),
                        task("c", "tr_c", "a", "b"),
                    ]
                },
            },
        )

        label_of = {edge.target.node_id: edge.target.label for edge in edges}
        by_target = {
            edge.target.label: {label_of[node_id] for node_id in edge.depends_on or ()}
            for edge in edges
            if edge.kind is FlowKind.CONTAINS
        }
        assert by_target == {"tr_a": set(), "tr_b": {"tr_a"}, "tr_c": {"tr_a", "tr_b"}}

    @pytest.mark.usefixtures("enable_data_products")
    def test_lineage_annotation_maps_uris(self) -> None:
        edges = _edges(
            WorkflowVersionIO,
            {
                "workflowExternalId": "wf",
                "version": "1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "a",
                            "type": "function",
                            "parameters": {"function": {"externalId": "fn"}},
                            "lineageAnnotation": {
                                "sources": [{"uri": "cdf://cluster/project/domain/d/timeseries/ts-1"}],
                                "targets": [{"uri": "cdf://cluster/project/other/shape"}],
                            },
                        }
                    ]
                },
            },
        )

        by_kind = {edge.kind: edge for edge in edges if edge.kind is not FlowKind.CONTAINS}
        assert by_kind[FlowKind.READS].target.type is TargetType.RESOURCE
        assert by_kind[FlowKind.READS].target.label == "ts-1"
        assert by_kind[FlowKind.WRITES].target.type is TargetType.EXTERNAL


class TestWorkflowTrigger:
    def _trigger(self, rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "externalId": "trigger",
            "triggerRule": rule,
            "workflowExternalId": "wf",
            "workflowVersion": "1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        }

    def test_data_modeling_trigger_reads_views(self) -> None:
        query = {
            "with": {
                "nodes": {
                    "nodes": {
                        "filter": {"hasData": [{"type": "view", "space": "sp", "externalId": "V", "version": "1"}]}
                    }
                }
            },
            "select": {"nodes": {}},
        }
        rule = {"triggerType": "dataModeling", "dataModelingQuery": query, "batchSize": 100, "batchTimeout": 60}

        edges = _edges(WorkflowTriggerIO, self._trigger(rule))

        assert _summary(edges) == {("triggers", "wf(v1)"), ("reads", "sp:V(v1)")}

    def test_unknown_query_shape_is_unresolved(self) -> None:
        rule = {
            "triggerType": "dataModeling",
            "dataModelingQuery": {"something": 1},
            "batchSize": 100,
            "batchTimeout": 60,
        }

        edges = _edges(WorkflowTriggerIO, self._trigger(rule))

        reads = [edge for edge in edges if edge.kind is FlowKind.READS]
        assert [edge.target.type for edge in reads] == [TargetType.UNRESOLVED]


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            {"hasData": [{"type": "view", "space": "s", "externalId": "v", "version": "1"}]},
            [ViewReference("s", "v", "1")],
        ),
        ({"source": {"type": "container", "space": "s", "externalId": "c"}}, None),
        ("not an object", None),
    ],
)
def test_find_view_references(query: Any, expected: list[ViewReference] | None) -> None:
    assert find_view_references(query) == expected
