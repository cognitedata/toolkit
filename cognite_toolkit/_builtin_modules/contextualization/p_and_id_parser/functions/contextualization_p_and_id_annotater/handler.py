import json
import time
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, Any
from hashlib import sha256
from datetime import datetime, timezone
from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite, Row
from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotationApply
from cognite.client import data_modeling as dm

from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel
import yaml


FUNCTION_ID = "p_and_id_annotater"
EXTRACTION_PIPELINE_EXTERNAL_ID = yaml.safe_load(Path("extraction_pipeline").read_text())["externalId"]
EXTERNAL_ID_LIMIT = 256


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


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    auto_reject_threshold: float = Field(gt=0.0, le=1.0)
    max_failed_attempts: int = Field(gt=0)


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


class ConfigState(BaseModel, alias_generator=to_camel):
    raw_database: str
    raw_table: str
    source_system: dm.DirectRelationReference

    @field_validator("source_system", mode="before")
    def pares_direct_relation(self, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    state: ConfigState
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
class AnnotationJob(BaseModel):
    file_id: dm.NodeId
    last_completed_entity_cursor: dict[str, str] = Field(default_factory=dict)
    last_entity_cursors: dict[str, str] = Field(default_factory=dict)
    failed_attempts: int = 0
    latest_job_id: int | None = None
    error_message: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient, file_id: dm.NodeId, state: ConfigState) -> "AnnotationJob":
        row = client.raw.rows.retrieve(db_name=state.raw_database, table_name=state.raw_table, key=cls._row_key(file_id))
        return cls._from_row(file_id, row)

    def write_to_cdf(self, client: CogniteClient, state: ConfigState) -> None:
        client.raw.rows.insert(db_name=state.raw_database, table_name=state.raw_table, row=self._to_row())

    @classmethod
    def _from_row(cls, file_id: dm.NodeId, row: Row | None) -> "AnnotationJob":
        data = row.columns if row is not None else {}
        return cls(file_id=file_id, **data)

    def _to_row(self) -> RowWrite:
        data = self.model_dump(exclude={"file_id"})
        return RowWrite(key=self._row_key(self.file_id), columns=data)

    @classmethod
    def _row_key(cls, file_id: dm.NodeId) -> str:
        return f"{file_id.space}:{file_id.external_id}"


class Entity(BaseModel, alias_generator=to_camel, extra="allow"):
    node_id: dm.NodeId
    view_id: dm.ViewId

    @classmethod
    def from_nodes(cls, nodes: dm.NodeList) -> "list[Entity]":
        return [cls.from_node(node) for node in nodes]

    @classmethod
    def from_node(cls, node: dm.Node) -> "Entity":

        view_id, properties = next(iter(node.properties.items()))

        return cls(nodeId=node.as_id(), viewId=view_id, **properties)

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
    for job, result in wait_for_completion(jobs, client, config.state, logger):
        annotations = write_annotations(job, result, client, config.data.annotation_space, config.state.source_system, config.parameters, logger)
        annotation_count += len(annotations)

        job.last_completed_entity_cursor = job.last_entity_cursors
        job.latest_job_id = None
        job.failed_attempts = 0
        job.write_to_cdf(client, config.state)

    logger.info(f"Annotations created: {annotation_count}")


def trigger_diagram_detection_jobs(client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> list[AnnotationJob]:
    # Reshape configuration for easier processing
    entity_sources_by_file_view: dict[dm.ViewId, list[ViewProperty]] = defaultdict(list)
    for mapping in config.data.mappings:
        file_view = mapping.file_source.as_view_id()
        entity_sources_by_file_view[file_view].append(mapping.entity_source)

    instance_spaces = config.data.instance_spaces
    max_failed_attempts = config.parameters.max_failed_attempts

    # Trigger detection jobs for each file view
    jobs: list[AnnotationJob] = []
    for file_view, entity_sources in entity_sources_by_file_view.items():
        is_view = dm.filters.HasData(views=[file_view])
        for file_node in client.data_modeling.instances(instance_type="node", space=instance_spaces, filter=is_view):
            file_id = file_node.as_id()
            logger.debug(f"Processing file {file_id}")

            job = AnnotationJob.from_cdf(client, file_id, config.state)

            if job.failed_attempts >= max_failed_attempts:
                logger.warning(f"Failed to detect diagram for {file_id} "
                               f"after {max_failed_attempts} failed attempts. Will not try again.")
                continue

            job_id = trigger_detection_job(job, client, entity_sources, logger)
            job.latest_job_id = job_id

            job.write_to_cdf(client, config.state)
            jobs.append(job)
    return jobs


def trigger_detection_job(job: AnnotationJob, client: CogniteClient, entity_sources: list[ViewProperty], logger: CogniteFunctionLogger) -> int | None:
    query = create_entity_query(entity_sources, job.last_completed_entity_cursor)

    query_result = client.data_modeling.instances.sync(query)
    job.last_entity_cursors = query_result.cursors
    node_entities = dm.NodeList([
        node
        for nodes in query_result.values()
        for node in nodes
    ])

    logger.debug(f"Query executed, got {len(node_entities)} entities")

    if not node_entities:
        logger.info(f"No new entities found for {job.file_id}")
        return None

    entities = Entity.from_nodes(node_entities)

    # Rename search property to name so we can search for all entities simultaneously
    # and then map the results back to the original entities
    source_by_view = {source.as_view_id(): source for source in entity_sources}
    dumped_list: list[dict[str,Any]] = []
    for entity in entities:
        dumped = entity.model_dump(by_alias=True)
        source = source_by_view[entity.view_id]
        dumped["name"] = dumped.pop(source.search_property)
        dumped_list.append(dumped)

    diagram_result = client.diagrams.detect(
        entities=dumped_list,
        search_field="name",
        file_instance_ids=[job.file_id],
        partial_match=True,
        min_tokens=2
    )
    return diagram_result.job_id

def create_entity_query(entity_sources: list[ViewProperty], cursors: dict[str, str]) -> dm.query.Query:
    return dm.query.Query(
        with_={
            entity.external_id: dm.query.NodeResultSetExpression(
                from_=None,
                filter=dm.filters.HasData(views=[entity.as_view_id()]),
            )
             for entity in entity_sources
        },
        select={
            entity.external_id: dm.query.Select(
                sources=[dm.query.SourceSelector(source=entity.as_view_id(), properties=[entity.search_property]) for entity in entity_sources],
            )
            for entity in entity_sources
        },
        cursors={
            entity.external_id: cursors.get(entity.external_id)
            for entity in entity_sources
        }
    )


def wait_for_completion(jobs: list[AnnotationJob], client: CogniteClient, state: ConfigState, logger: CogniteFunctionLogger) -> Iterable[tuple[AnnotationJob, DiagramDetectResults]]:
    while jobs:
        job = jobs.pop(0)
        if job.latest_job_id is None:
            continue

        job_result = client.diagrams.get_detect_jobs([job.latest_job_id])[0]
        status = job_result.status.casefold()
        if status == "completed":
            yield job, job_result
        elif status in ("failed", "timeout"):
            logger.warning(f"Job {job.latest_job_id} {status}")
            job.failed_attempts += 1
            job.error_message = job_result.error_message
            job.write_to_cdf(client, state)
        else:
            jobs.append(job)
            logger.debug(f"Job {job.latest_job_id} {status}, will check again later")
            # Sleep for a bit to avoid hammering the API
            time.sleep(10)


def write_annotations(job: AnnotationJob, result: DiagramDetectResults, client: CogniteClient, annotation_space: str, source: dm.DirectRelationReference, parameter: Parameters, logger: CogniteFunctionLogger) -> list[CogniteDiagramAnnotationApply]:
    annotation_list: list[CogniteDiagramAnnotationApply] = []
    for detection in result.items:
        for raw_annotation in detection.annotations or []:
            entities = Entity.from_annotation(raw_annotation)
            for entity in entities:
                annotation = load_annotation(raw_annotation, entity, job.file_id, annotation_space, source, parameter)
                annotation_list.append(annotation)

    created = client.data_modeling.instances.apply(annotation_list).edges

    create_count = sum([1 for result in created if result.was_modified and result.created_time == result.last_updated_time])
    update_count = sum([1 for result in created if result.was_modified and result.created_time != result.last_updated_time])
    unchanged_count = len(created) - create_count - update_count
    logger.info(f"Created {create_count} updated {update_count}, and {unchanged_count} unchanged annotations for {job.file_id}")
    return annotation_list


def load_annotation(raw_annotation: dict[str, Any], entity: Entity, file_id: dm.NodeId, annotation_space: str, source: dm.DirectRelationReference, parameters: Parameters) -> CogniteDiagramAnnotationApply:
        text = raw_annotation["text"]
        external_id = create_annotation_id(file_id, entity.node_id, text, raw_annotation)
        confidence = raw_annotation["confidence"] if "confidence" in raw_annotation else None
        status: Literal["Approved", "Suggested", "Rejected"] = "Suggested"
        if confidence is not None and confidence >= parameters.auto_approval_threshold:
            status = "Approved"
        elif confidence is not None and confidence <= parameters.auto_reject_threshold:
            status = "Rejected"
        region = raw_annotation["region"]
        vertices = region["vertices"]
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return CogniteDiagramAnnotationApply(
            space=annotation_space,
            external_id=external_id,
            start_node=(file_id.space, file_id.external_id),
            end_node=(entity.node_id.space, entity.node_id.external_id),
            type=(annotation_space, entity.view_id.external_id),
            name=text,
            confidence=confidence,
            status=status,
            start_node_text=text,
            start_node_page_number=region["page"],
            start_node_x_min=min(v["x"] for v in vertices),
            start_node_x_max=max(v["x"] for v in vertices),
            start_node_y_min=min(v["y"] for v in vertices),
            start_node_y_max=max(v["y"] for v in vertices),
            source=source,
            source_created_time=now,
            source_updated_time=now,
            source_created_user=FUNCTION_ID,
            source_updated_user=FUNCTION_ID,
            source_context=json.dumps({"source": entity.view_id.dump()})
        )


def create_annotation_id(file_id: dm.NodeId, node_id: dm.NodeId, text, raw_annotation: dict[str, Any]) -> str:
    hash_ = sha256(json.dumps(raw_annotation, sort_keys=True).encode()).hexdigest()[:10]
    naive = f"{file_id.space}:{file_id.external_id}:{node_id.space}:{node_id.external_id}:{text}:{hash_}"
    if len(naive) < EXTERNAL_ID_LIMIT:
        return naive
    prefix = f"{file_id.external_id}:{node_id.external_id}:{text}"
    shorten = f"{prefix}:{hash_}"
    if len(shorten) < EXTERNAL_ID_LIMIT:
        return shorten
    return prefix[:EXTERNAL_ID_LIMIT - 10] + hash_


def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e