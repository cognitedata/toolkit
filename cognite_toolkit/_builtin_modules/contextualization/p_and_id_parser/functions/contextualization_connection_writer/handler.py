import json
from collections.abc import Iterable, Sequence
from typing import Literal, ClassVar, TypeVar

import yaml

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRunWrite
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotationApply
from mypy.checkexpr import defaultdict
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
        ExtractionPipelineRunWrite(
            extpipe_external_id=EXTRACTION_PIPELINE_EXTERNAL_ID,
            status=status,
            message=message
        )
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


class Config(BaseModel, alias_generator=to_camel):
    annotation_space: str


class State(BaseModel):
    key: ClassVar[str] = FUNCTION_ID
    last_cursor: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient) -> "State":
        row = client.raw.rows.retrieve(database=RAW_DATABASE, table=RAW_TABLE, key=cls.key)
        if row is None:
            return cls()
        return cls.model_validate(row.columns)

    def to_cdf(self, client: CogniteClient) -> None:
        client.raw.rows.insert(
            database=RAW_DATABASE,
            table=RAW_TABLE,
            row_key=self.key,
            columns=self.model_dump()
        )


################# Functions #################

def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO")) # type: ignore[arg-type]
    logger.debug("Starting connection write")
    config = load_config(client, logger)
    logger.debug("Loaded config successfully")

    state = State.from_cdf(client)
    connection_count = 0
    for annotation_list in iterate_new_approved_annotations(state, client, config, logger):
        connections = write_connections(annotation_list, client, logger)
        connection_count += connections

    state.to_cdf(client)
    logger.info(f"Created {connection_count} connections")

T = TypeVar("T")

def chunker(items: Sequence[T], chunk_size: int) -> Iterable[list[T]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def iterate_new_approved_annotations(state: State, client: CogniteClient, config: Config, logger: CogniteFunctionLogger, chunk_size: int=1000) -> Iterable[list[CogniteDiagramAnnotationApply]]
    query = create_query(state.last_cursor, config.annotation_space)
    edges = client.data_modeling.instances.sync(query)
    logger.debug(f"Retrieved {len(edges)} new annotations")
    state.last_cursor = edges.cursor
    for edge_list in chunker(edges, chunk_size):
        yield [CogniteDiagramAnnotationApply._load(edge.dump()) for edge in edge_list]


def write_connections(annotations: list[CogniteDiagramAnnotationApply], client: CogniteClient, logger: CogniteFunctionLogger) -> int:
    annotation_by_source_by_node: dict[dm.ViewId, dict[dm.NodeId, list[CogniteDiagramAnnotationApply]]] = defaultdict(lambda: defaultdict(list))
    for annotation in annotations:
        try:
            source = dm.ViewId.load(json.loads(annotation.source_context)["source"])
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Could not parse source context for annotation {annotation.external_id}")
            continue
        node = dm.NodeId(annotation.end_node.space, annotation.end_node.external_id)
        annotation_by_source_by_node[source][node].append(annotation)

    connection_count = 0
    for view_id, annotation_by_source_by_node in annotation_by_source_by_node.items():
        existing_node_list = client.data_modeling.instances.retrieve(list(annotation_by_source_by_node.keys()), sources=[view_id]).nodes
        existing_node_by_id = {node.as_id(): node.as_write() for node in existing_node_list}
        updated_nodes: list[dm.NodeApply] = []
        for node_id, annotations in annotation_by_source_by_node.items():
            existing_node: dm.NodeApply = existing_node_by_id.get(node_id)
            if existing_node is None:
                logger.warning(f"Node {node_id} not found in view {view_id}")
                continue
            for source in existing_node.sources:
                if source.source == view_id:

                    source.properties["whatever"].append({annotations.dump()})
            updated_nodes.append(existing_node)
    updated = client.data_modeling.instances.apply(updated_nodes)
    logger.debug(f"Updated {updated} nodes")

    return connection_count



def create_query(last_query: str | None, annotation_space) -> dm.query.Query:
    is_annotation = dm.filters.And(
        dm.filters.Equals(["edge", "space"], annotation_space),
        dm.filters.HasData(views=[CogniteDiagramAnnotationApply.get_source()]),
        dm.filters.Equals(["edge", "status"], "Approved"),
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
                [dm.query.SourceSelector(
                source=CogniteDiagramAnnotationApply.get_source(),
                properties=["*"])]
            )
        }
    )


def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
