import json
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import ClassVar, Literal, TypeVar

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotation, CogniteDiagramAnnotationApply
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

FUNCTION_ID = "connection_writer"
EXTRACTION_PIPELINE_EXTERNAL_ID = "p_and_id_parser"
RAW_DATABASE = "contextualizationState"
RAW_TABLE = "diagramParsing"
EXTERNAL_ID_LIMIT = 256
SOURCE_ID = dm.DirectRelationReference("sp_p_and_id_parser", "p_and_id_parser")


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        execute(data, client)
    except Exception as e:
        status: Literal["failure", "success"] = "failure"
        # Truncate the error message to 1000 characters the maximum allowed by the API
        message = f"ERROR {FUNCTION_ID}: {e!s}"[:1000]
    else:
        status = "success"
        message = FUNCTION_ID

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRunWrite(extpipe_external_id=EXTRACTION_PIPELINE_EXTERNAL_ID, status=status, message=message)
    )
    # Need to run at least daily or the sync endpoint will forget the cursors
    # (max time is 3 days).
    return {"status": status, "message": message}


################# Data Classes #################


# Logger using print
class CogniteFunctionLogger:
    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"):
        self.log_level = log_level.upper()

    def debug(self, message: str):
        if self.log_level == "DEBUG":
            print(f"[DEBUG] {message}")

    def info(self, message: str):
        if self.log_level in ("DEBUG", "INFO"):
            print(f"[INFO] {message}")

    def warning(self, message: str):
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            print(f"[WARNING] {message}")

    def error(self, message: str):
        print(f"[ERROR] {message}")


class ViewProperty(BaseModel, alias_generator=to_camel):
    space: str
    external_id: str
    version: str
    direct_relation_property: str | None = None
    search_property: str = "name"

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class Mapping(BaseModel, alias_generator=to_camel):
    file_source: ViewProperty
    entity_source: ViewProperty


class ConfigData(BaseModel, alias_generator=to_camel):
    instance_spaces: list[str]
    annotation_space: str
    mappings: list[Mapping]


class Config(BaseModel, alias_generator=to_camel):
    data: ConfigData


class State(BaseModel):
    key: ClassVar[str] = FUNCTION_ID
    last_cursor: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient) -> "State":
        row = client.raw.rows.retrieve(db_name=RAW_DATABASE, table_name=RAW_TABLE, key=cls.key)
        if row is None:
            return cls()
        return cls.model_validate(row.columns)

    def to_cdf(self, client: CogniteClient) -> None:
        client.raw.rows.insert(
            db_name=RAW_DATABASE,
            table_name=RAW_TABLE,
            row=self._as_row(),
        )

    def _as_row(self) -> RowWrite:
        return RowWrite(
            key=self.key,
            columns=self.model_dump(),
        )


################# Functions #################


def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO"))  # type: ignore[arg-type]
    logger.debug("Starting connection write")
    config = load_config(client, logger)
    logger.debug("Loaded config successfully")

    state = State.from_cdf(client)
    connection_count = 0
    for annotation_list in iterate_new_approved_annotations(state, client, config.data.annotation_space, logger):
        annotation_by_source_by_node = to_direct_relations_by_source_by_node(
            annotation_list, config.data.mappings, logger
        )
        connections = write_connections(annotation_by_source_by_node, client, logger)
        connection_count += connections

    state.to_cdf(client)
    logger.info(f"Created {connection_count} connections")


T = TypeVar("T")


def chunker(items: Sequence[T], chunk_size: int) -> Iterable[list[T]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def iterate_new_approved_annotations(
    state: State, client: CogniteClient, annotation_space: str, logger: CogniteFunctionLogger, chunk_size: int = 1000
) -> Iterable[list[CogniteDiagramAnnotation]]:
    query = create_query(state.last_cursor, annotation_space)
    result = client.data_modeling.instances.sync(query)
    edges = result["annotations"]
    logger.debug(f"Retrieved {len(edges)} new approved annotations")
    state.last_cursor = edges.cursor
    for edge_list in chunker(list(edges), chunk_size):
        yield [CogniteDiagramAnnotation._load(edge.dump()) for edge in edge_list]


def write_connections(
    annotation_by_source_by_node: dict[dm.ViewId, dict[(dm.NodeId, str), list[dm.DirectRelationReference]]],
    client: CogniteClient,
    logger: CogniteFunctionLogger,
) -> int:
    connection_count = 0
    updated_nodes: list[dm.NodeApply] = []
    for view_id, annotation_by_source_by_node in annotation_by_source_by_node.items():
        node_ids = [node_id for node_id, _ in annotation_by_source_by_node.keys()]
        existing_node_list = client.data_modeling.instances.retrieve(node_ids, sources=[view_id]).nodes
        existing_node_by_id = {node.as_id(): node.as_write() for node in existing_node_list}

        for (node_id, direct_relation_property), direct_relation_ids in annotation_by_source_by_node.items():
            existing_node = existing_node_by_id.get(node_id)
            if existing_node is None:
                logger.warning(f"Node {node_id} not found in view {view_id}")
                continue
            for entity_source in existing_node.sources:
                if entity_source.source == view_id:
                    existing_connections = entity_source.properties.get(direct_relation_property, [])
                    before = len(existing_connections)
                    all_connections = {
                        dm.DirectRelationReference.load(connection) for connection in existing_connections
                    } | set(direct_relation_ids)
                    after = len(all_connections)
                    entity_source.properties[direct_relation_property] = [
                        connection.dump() for connection in all_connections
                    ]
                    connection_count += after - before
                    break
            updated_nodes.append(existing_node)

    updated = client.data_modeling.instances.apply(updated_nodes)
    logger.debug(f"Updated {len(updated.nodes)} nodes")
    return connection_count


def to_direct_relations_by_source_by_node(
    annotations: list[CogniteDiagramAnnotation], mappings: list[Mapping], logger: CogniteFunctionLogger
) -> dict[dm.ViewId, dict[(dm.NodeId, str), list[dm.DirectRelationReference]]]:
    mapping_by_entity_source: dict[dm.ViewId, Mapping] = {
        mapping.entity_source.as_view_id(): mapping for mapping in mappings
    }
    annotation_by_source_by_node: dict[dm.ViewId, dict[(dm.NodeId, str), list[dm.DirectRelationReference]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for annotation in annotations:
        try:
            entity_source = dm.ViewId.load(json.loads(annotation.source_context)["source"])
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Could not parse source context for annotation {annotation.external_id}")
            continue
        mapping = mapping_by_entity_source.get(entity_source)
        if mapping.file_source.direct_relation_property is not None:
            update_node = annotation.start_node
            direct_relation_property = mapping.file_source.direct_relation_property
            other_side = annotation.end_node
            view_id = mapping.file_source.as_view_id()
        elif mapping.entity_source.direct_relation_property is not None:
            update_node = annotation.end_node
            direct_relation_property = mapping.entity_source.direct_relation_property
            other_side = annotation.start_node
            view_id = mapping.entity_source.as_view_id()
        else:
            raise ValueError(
                f"Neither file source nor entity source has a direct relation property for annotation {annotation.external_id}"
            )
        node = dm.NodeId(update_node.space, update_node.external_id)
        annotation_by_source_by_node[view_id][(node, direct_relation_property)].append(
            dm.DirectRelationReference(other_side.space, other_side.external_id)
        )
    return annotation_by_source_by_node


def create_query(last_cursor: str | None, annotation_space: str) -> dm.query.Query:
    view_id = CogniteDiagramAnnotationApply.get_source()
    is_annotation = dm.filters.And(
        dm.filters.Equals(["edge", "space"], annotation_space),
        dm.filters.HasData(views=[view_id]),
        dm.filters.Equals(view_id.as_property_ref("status"), "Approved"),
    )
    return dm.query.Query(
        with_={
            "annotations": dm.query.EdgeResultSetExpression(
                from_=None,
                filter=is_annotation,
            )
        },
        select={
            "annotations": dm.query.Select(
                [
                    dm.query.SourceSelector(
                        source=CogniteDiagramAnnotationApply.get_source(), properties=["sourceContext"]
                    )
                ]
            )
        },
        cursors={"annotations": last_cursor},
    )


def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
