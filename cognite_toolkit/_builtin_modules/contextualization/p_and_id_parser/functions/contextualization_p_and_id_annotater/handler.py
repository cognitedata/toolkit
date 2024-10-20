import time
from collections.abc import Iterable
from typing import Literal, cast, Any
from hashlib import sha256
from datetime import datetime, timezone
from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite, Row
from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotationApply
from cognite.client import data_modeling as dm
from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel
import yaml
from win32ctypes.pywin32.pywintypes import datetime

FUNCTION_ID = "p_and_id_annotater"
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
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    max_failed_attempts: int = Field(gt=0)


class ConfigData(BaseModel, alias_generator=to_camel):
    instance_spaces: list[str]
    input_file_views: list[dm.ViewId]
    entity_views: list[dm.ViewId]


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData
    annotation_space: str


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
class ParsingJob(BaseModel):
    file_id: dm.NodeId
    last_completed_entity_cursor: str | None = None
    last_entity_cursor: str | None = None
    failed_attempts: int = 0
    latest_job_id: int | None = None
    error_message: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient, file_id: dm.NodeId) -> "ParsingJob":
        row = client.raw.rows.retrieve(database=RAW_DATABASE, table=RAW_TABLE, key=cls._row_key(file_id))
        return cls._from_row(file_id, row)

    def write_to_cdf(self, client: CogniteClient) -> None:
        client.raw.rows.insert(database=RAW_DATABASE, table=RAW_TABLE, row=self._to_row())

    @classmethod
    def _from_row(cls, file_id: dm.NodeId, row: Row | None) -> "ParsingJob":
        data = row.columns if row is not None else {}
        return cls(file_id=file_id, **data)

    def _to_row(self) -> RowWrite:
        data = self.model_dump(exclude={"file_id"})
        return RowWrite(key=self.row_key,columns=data)

    @classmethod
    def _row_key(cls, file_id: dm.NodeId) -> str:
        return f"{file_id.space}:{file_id.external_id}"


class Entity(BaseModel, alias_generator=to_camel):
    node_id: dm.NodeId
    view_id: dm.ViewId
    name: str

    @classmethod
    def from_nodes(cls, nodes: dm.NodeListWithCursor) -> "list[Entity]":
        return [cls.from_node(node) for node in nodes]

    @classmethod
    def from_node(cls, node: dm.Node) -> "Entity":

        view_id, properties = next(iter(node.properties.items()))

        return cls(node_id=node.as_id(), view_id=view_id, **properties)

    @classmethod
    def from_annotation(cls, data) -> "list[Entity]":
        return [cls.model_validate(item) for item in data["entities"]]


#####################################################

################# Functions #################

def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO")) # type: ignore[arg-type]
    logger.debug("Starting diagram parsing annotation")
    config = load_config(client, logger)
    logger.debug("Loaded config successfully")

    jobs = trigger_diagram_detection_jobs(client, config, logger)

    logger.info(f"Detection jobs created: {len(jobs)}")

    annotation_count = 0
    for job, result in wait_for_completion(jobs, client, logger):
        annotations = write_annotations(job, result, client, config, logger)
        annotation_count += len(annotations)

        job.last_completed_entity_cursor = job.last_entity_cursor
        job.latest_job_id = None
        job.failed_attempts = 0
        job.write_to_cdf(client)

    logger.info(f"Annotations created: {annotation_count}")


def trigger_diagram_detection_jobs(client: CogniteClient, config: Config,logger: CogniteFunctionLogger) -> list[ParsingJob]:
    jobs: list[ParsingJob] = []
    for file_view in config.data.input_file_views:
        is_view = dm.filters.HasData(views=[file_view])
        for file_node in client.data_modeling.instances("node", chunk_size=None, space=config.data.instance_spaces, filter=is_view):
            file_id = file_node.as_id()
            logger.debug(f"Processing file {file_id}")
            job = ParsingJob.from_cdf(client, file_id)

            if job.failed_attempts >= config.parameters.max_failed_attempts:
                logger.warning(f"Failed to detect diagram for {file_id} "
                               f"after {config.parameters.max_failed_attempts} failed attempts. Will not try again.")
                continue

            job_id = trigger_detection_job(job, client, config, logger)
            job.latest_job_id = job_id

            job.write_to_cdf(client)
            jobs.append(job)
    return jobs


def wait_for_completion(jobs: list[ParsingJob], client: CogniteClient, logger: CogniteFunctionLogger) -> Iterable[tuple[ParsingJob, DiagramDetectResults]]:
    while jobs:
        job = jobs.pop(0)
        if job.latest_job_id is None:
            continue

        job_result = client.diagrams.get_detect_jobs(job.latest_job_id)[0]
        status = job_result.status.casefold()
        if status == "completed":
            yield job, job_result
        elif status in ("failed", "timeout"):
            logger.warning(f"Job {job.latest_job_id} {status}")
            job.failed_attempts += 1
            job.error_message = job_result.error_message
            job.write_to_cdf(client)
        else:
            jobs.append(job)
            logger.debug(f"Job {job.latest_job_id} {status}, will check again later")
            # Sleep for a bit to avoid hammering the API
            time.sleep(10)


def write_annotations(job: ParsingJob, result: DiagramDetectResults, client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> list[CogniteDiagramAnnotationApply]:
    annotation_list: list[CogniteDiagramAnnotationApply] = []
    for detection in result.items:
        for raw_annotation in detection.annotations or []:
            entities = Entity.from_annodation(raw_annotation)
            for entity in entities:
                annotation = load_annotation(raw_annotation, entity, job.file_id, config)
                annotation_list.append(annotation)

    created = client.data_modeling.instances.apply(annotation_list).nodes

    create_count = sum([1 for result in created if result.was_modified and result.created_time == result.last_updated_time])
    update_count = sum([1 for result in created if result.was_modified and result.created_time != result.last_updated_time])

    logger.info(f"Created {create_count} and updated {update_count} annotations for {job.file_id}")
    return annotation_list


def load_annotation(raw_annotation: dict[str, Any], entity: Entity, file_id: dm.NodeId, config: Config) -> CogniteDiagramAnnotationApply:
        text = raw_annotation["text"]
        external_id = create_annotation_id(file_id, entity.node_id, text)
        confidence = raw_annotation["confidence"] if "confidence" in raw_annotation else None
        status: Literal["Approved", "Suggested"] = "Suggested"
        if confidence is not None and confidence >= config.parameters.auto_approval_threshold:
            status = "Approved"
        vertices = raw_annotation["vertices"]
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return CogniteDiagramAnnotationApply(
            space=config.annotation_space,
            external_id=external_id,
            start_node=(file_id.space, file_id.external_id),
            end_node=(entity.node_id.space, entity.node_id.external_id),
            type=(config.annotation_space, entity.view_id.external_id),
            name=text,
            confidence=confidence,
            status=status,
            start_node_text=text,
            start_node_page_number=raw_annotation["page"],
            start_node_x_min=min(v["x"] for v in vertices),
            start_node_x_max=max(v["x"] for v in vertices),
            start_node_y_min=min(v["y"] for v in vertices),
            start_node_y_max=max(v["y"] for v in vertices),
            source=SOURCE_ID,
            source_created_time=now,
            source_updated_time=now,
            source_created_user=FUNCTION_ID,
            source_updated_user=FUNCTION_ID,
        )


def create_annotation_id(file_id: dm.NodeId, node_id: dm.NodeId, text: str) -> str:
    naive = f"{file_id.space}:{file_id.external_id}:{node_id.space}:{node_id.external_id}:{text}"
    if len(naive) < EXTERNAL_ID_LIMIT:
        return naive
    hash_ = sha256(naive.encode()).hexdigest()[:10]
    prefix = f"{file_id.external_id}:{node_id.external_id}:{text}"
    shorten = f"{prefix}:{hash_}"
    if len(shorten) < EXTERNAL_ID_LIMIT:
        return shorten
    return shorten[:EXTERNAL_ID_LIMIT - 10] + hash_



def trigger_detection_job(job: ParsingJob, client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> int | None:
    query = create_entity_query(config, job.last_completed_entity_cursor)

    query_result = client.data_modeling.instances.sync(query)
    node_entities = cast(dm.NodeListWithCursor, query_result["entities"])
    job.last_entity_cursor = node_entities.cursor

    logger.debug(f"Query executed, got {len(node_entities)} entities")

    if not node_entities:
        logger.info(f"No new entities found for {job.file_id}")
        return None

    entities = Entity.from_nodes(node_entities)
    diagram_result = client.diagrams.detect(
        entities=[entity.model_dump(by_alias=True) for entity in entities],
        search_field="name",
        file_instance_ids=[job.file_id],
        partial_match=True,
        min_tokens=2
    )
    return diagram_result.job_id

def create_entity_query(config: Config, cursor: str | None) -> dm.query.Query:
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
            "entities": cursor,
        }
    )

def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
