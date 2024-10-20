from __future__ import annotations

from typing import Literal, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite, Row
from cognite.client import data_modeling as dm
from pydantic import BaseModel
from pydantic.alias_generators import to_camel
import yaml

EXTRACTION_PIPELINE_EXTERNAL_ID = "p_and_id_parser"
RAW_DATABASE = "contextualizationState"
RAW_TABLE = "diagramParsing"


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        execute(data, client)
    except Exception as e:
        status: Literal["failure", "success"] = "failure"
        # Truncate the error message to 1000 characters the maximum allowed by the API
        message = f"ERROR: {e!s}"[:1000]
    else:
        status = "success"
        message = ""

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
# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    match_threshold: float
    max_failed_attempts: int


class ConfigData(BaseModel, alias_generator=to_camel):
    instance_spaces: list[str]
    input_file_views: list[dm.ViewId]
    entity_views: list[dm.ViewId]


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData

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


# Diagram Parsing State
class ParsingState(BaseModel):
    file_id: dm.NodeId
    cursor: str | None = None
    failed_attempts: int = 0
    latest_job_id: int | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient, file_id: dm.NodeId) -> ParsingState:
        row = client.raw.rows.retrieve(database=RAW_DATABASE, table=RAW_TABLE, key=cls._row_key(file_id))
        return cls._from_row(file_id, row)

    def write_to_cdf(self, client: CogniteClient) -> None:
        client.raw.rows.insert(database=RAW_DATABASE, table=RAW_TABLE, row=self._to_row())

    @classmethod
    def _from_row(cls, file_id: dm.NodeId, row: Row | None) -> ParsingState:
        data = row.columns if row is not None else {}
        return cls(file_id=file_id, **data)

    def _to_row(self) -> RowWrite:
        data = self.model_dump(exclude={"file_id"})
        return RowWrite(key=self.row_key,columns=data)

    @classmethod
    def _row_key(cls, file_id: dm.NodeId) -> str:
        return f"{file_id.space}:{file_id.external_id}"


class Entity(BaseModel):
    node_id: dm.NodeId
    view_id: dm.ViewId
    name: str

    @classmethod
    def from_nodes(cls, nodes: dm.NodeListWithCursor) -> list[Entity]:
        return [cls.from_node(node) for node in nodes]

    @classmethod
    def from_node(cls, node: dm.Node) -> Entity:

        view_id, properties = next(iter(node.properties.items()))

        return cls(node_id=node.as_id(), view_id=view_id, **properties)

#####################################################

################# Functions #################

def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO"))
    logger.debug("Starting diagram parsing contextualization")
    config = load_config(client)
    logger.debug("Loaded config")

    states = trigger_diagram_detection_jobs(client, config, logger)

    connections = write_annotations(states, client, config, logger)
    logger.info(f"Connections created: {connections}")

    write_connections(connections, client, logger)

    logger.info("Contextualization completed")



def trigger_diagram_detection_jobs(client: CogniteClient, config: Config,logger: CogniteFunctionLogger) -> list[ParsingState]:
    jobs: list[ParsingState] = []
    for file_view in config.data.input_file_views:
        is_view = dm.filters.HasData(views=[file_view])
        for file_node in client.data_modeling.instances("node", chunk_size=None, space=config.data.instance_spaces, filter=is_view):
            file_id = file_node.as_id()
            logger.debug(f"Processing file {file_id}")
            state = ParsingState.from_cdf(client, file_id)

            if state.failed_attempts >= config.parameters.max_failed_attempts:
                logger.warning(f"Failed to detect diagram for {file_id} "
                               f"after {config.parameters.max_failed_attempts} failed attempts. Will not try again.")
                continue

            job_id = trigger_detection_job(state, client, config, logger)
            state.latest_job_id = job_id

            state.write_to_cdf(client)
            jobs.append(state)
    return jobs


def write_annotations(states: list[ParsingState], client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> list[Entity]:




def write_connections(connections: list[Entity], client: CogniteClient, logger: CogniteFunctionLogger) -> None:
    raise NotImplementedError("write_connections")

def trigger_detection_job(state: ParsingState, client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> int | None:
    query = create_entity_query(config, state)

    query_result = client.data_modeling.instances.sync(query)
    node_entities = cast(dm.NodeListWithCursor, query_result["entities"])
    state.cursor = node_entities.cursor

    logger.debug(f"Query executed, got {len(node_entities)} entities")

    if not node_entities:
        logger.info(f"No new entities found for {state.file_id}")
        return None

    entities = Entity.from_nodes(node_entities)
    diagram_result = client.diagrams.detect(
        entities=entities.dump(),
        search_field="name",
        file_instance_ids=[state.file_id],
        partial_match=True,
        min_tokens=2
    )
    return diagram_result.job_id

def create_entity_query(config: Config, state: ParsingState) -> dm.query.Query:
    return dm.query.Query(
        with_={
            "entities": dm.query.NodeResultSetExpression(
                from_=None,
                filter=dm.filters.HasData(views=config.data.entity_views)
            )
        },
        select={
            "entities": dm.query.Select(
                sources=[dm.query.SourceSelector(source=view_id, properties=["name"]) for view_id in
                         config.data.entity_views],
            )
        },
        cursors={
            "entities": state.cursor,
        }
    )

def load_config(client: CogniteClient) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    return Config.model_validate(yaml.safe_load(raw_config.config))
