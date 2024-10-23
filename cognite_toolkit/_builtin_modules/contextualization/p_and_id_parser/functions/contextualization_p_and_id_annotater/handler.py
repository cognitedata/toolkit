import json
import time
import traceback
from collections.abc import Iterable
from typing import Literal, Any
from hashlib import sha256
from datetime import datetime, timezone
from cognite.client.config import global_config


# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True
from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRunWrite
from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotationApply
from cognite.client import data_modeling as dm

from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel
import yaml

FUNCTION_ID = "p_and_id_annotater"
EXTRACTION_PIPELINE_EXTERNAL_ID = "ctx_files_pandid_annotater"
EXTERNAL_ID_LIMIT = 256
MAX_FILES_PER_JOB = 50
EXTRACTION_RUN_MESSAGE_LIMIT = 1000


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        execute(data, client)
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        last_entry_this_file = next((entry for entry in reversed(tb) if entry.filename == __file__), None)
        suffix = ""
        if last_entry_this_file:
            suffix = f" in function {last_entry_this_file.name} on line {last_entry_this_file.lineno}: {last_entry_this_file.line}"

        status: Literal["failure", "success"] = "failure"
        # Truncate the error message to 1000 characters the maximum allowed by the API
        prefix = f"ERROR {FUNCTION_ID}: "
        error_msg = f'"{e!s}"'
        message = prefix + error_msg + suffix
        if len(message) >= EXTRACTION_RUN_MESSAGE_LIMIT:
            error_msg = error_msg[:EXTRACTION_RUN_MESSAGE_LIMIT - len(prefix) - len(suffix)- 3]
            message = prefix + error_msg + '..."' + suffix
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
    return {"status": status, "message": message}

################# Data Classes #################


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    auto_reject_threshold: float = Field(gt=0.0, le=1.0)


class ViewProperty(BaseModel, alias_generator=to_camel):
    space: str
    external_id: str
    version: str
    search_property: str = "name"

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class AnnotationJobConfig(BaseModel, alias_generator=to_camel):
    file_source: ViewProperty
    entity_views: list[ViewProperty]


class ConfigData(BaseModel, alias_generator=to_camel):
    instance_spaces: list[str]
    annotation_space: str
    annotation_jobs: list[AnnotationJobConfig]


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData
    source_system: dm.DirectRelationReference

    @classmethod
    @field_validator("source_system", mode="before")
    def pares_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


# Logger using print
class CogniteFunctionLogger:
    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"):
        self.log_level = log_level.upper()

    def _print(self, prefix: str, message: str) -> None:
        if "\n" not in message:
            print(f"{prefix} {message}")
            return
        lines = message.split("\n")
        print(f"{prefix} {lines[0]}")
        prefix_len = len(prefix)
        for line in lines[1:]:
            print(f"{' ' * prefix_len} {line}")

    def debug(self, message: str):
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)

    def info(self, message: str):
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)

    def warning(self, message: str):
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)

    def error(self, message: str):
        self._print("[ERROR]", message)


class Entity(BaseModel, alias_generator=to_camel, extra="allow"):
    node_id: dm.NodeId
    view_id: dm.ViewId
    name: str

    @classmethod
    def from_nodes(cls, nodes: dm.NodeList, search_property: str) -> "list[Entity]":
        return [cls.from_node(node, search_property) for node in nodes]

    @classmethod
    def from_node(cls, node: dm.Node, search_property: str) -> "Entity":

        view_id, properties = next(iter(node.properties.items()))

        return cls(nodeId=node.as_id(), viewId=view_id, name=properties[search_property])

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
    for result in wait_for_completion(jobs, logger):
        if result.errors:
            errors_str = '\n  - '.join(sorted(set(result.errors)))
            logger.error(f"Job {result.job_id} {len(result.errors)} files failed: \n  - {errors_str}")
            continue
        annotations = write_annotations(result, client, config.data.annotation_space, config.source_system, config.parameters, logger)
        annotation_count += len(annotations)

    logger.info(f"Annotations created: {annotation_count}")


def trigger_diagram_detection_jobs(client: CogniteClient, config: Config, logger: CogniteFunctionLogger) -> list[DiagramDetectResults]:
    instance_spaces = config.data.instance_spaces
    jobs: list[DiagramDetectResults] = []
    for job_config in config.data.annotation_jobs:
        file_view = job_config.file_source.as_view_id()
        is_view = dm.filters.HasData(views=[file_view])
        is_uploaded = dm.filters.Equals(file_view.as_property_ref("isUploaded"), True)
        is_file_type = dm.filters.In(file_view.as_property_ref("mimeType"), ['application/pdf', 'image/jpeg', 'image/png', 'image/tiff'])
        is_selected = dm.filters.And(is_view, is_uploaded, is_file_type)

        entities = get_entities(client, job_config, instance_spaces, logger)

        for file_list in client.data_modeling.instances(instance_type="node", space=instance_spaces, filter=is_selected, chunk_size=MAX_FILES_PER_JOB):
            file_ids = file_list.as_ids()

            diagram_result = client.diagrams.detect(
                entities=[entity.model_dump(by_alias=True) for entity in entities],
                search_field="name",
                file_instance_ids=file_ids,
                partial_match=True,
                min_tokens=2
            )
            jobs.append(diagram_result)
    return jobs


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

def get_entities(client: CogniteClient, job_config: AnnotationJobConfig, instance_spaces: list[str], logger: CogniteFunctionLogger) -> list[Entity]:
    entity_list: list[Entity] = []
    for entity_view in job_config.entity_views:
        for node_list in client.data_modeling.instances(chunk_size=1_000, instance_type="node", space=instance_spaces, sources=[entity_view.as_view_id()]):
            entity_list.extend(Entity.from_nodes(node_list, entity_view.search_property))
    logger.debug(f"Found {len(entity_list)} entities for {job_config.file_source.external_id}")
    return entity_list


def wait_for_completion(jobs: list[DiagramDetectResults], logger: CogniteFunctionLogger) -> Iterable[DiagramDetectResults]:
    # The Cognite Function will eventually time out, so we don't need to worry about running forever
    while jobs:
        job = jobs.pop(0)

        job.update_status()

        status = job.status.casefold()
        if status == "completed":
            yield job
        elif status in ("failed", "timeout"):
            logger.warning(f"Job {job.job_id} {status}: {job.error_message}")
        else:
            jobs.append(job)
            logger.debug(f"Job {job.job_id} {status}, will check again later")
            # Sleep for a bit to avoid hammering the API
            time.sleep(10)


def write_annotations(result: DiagramDetectResults, client: CogniteClient, annotation_space: str, source: dm.DirectRelationReference, parameter: Parameters, logger: CogniteFunctionLogger) -> list[CogniteDiagramAnnotationApply]:
    annotation_list: list[CogniteDiagramAnnotationApply] = []
    for detection in result.items:
        for raw_annotation in detection.annotations or []:
            entities = Entity.from_annotation(raw_annotation)
            for entity in entities:
                file_id = dm.NodeId.load(detection.file_instance_id)
                annotation = load_annotation(raw_annotation, entity, file_id, annotation_space, source, parameter)
                annotation_list.append(annotation)

    created = client.data_modeling.instances.apply(annotation_list).edges

    create_count = sum([1 for result in created if result.was_modified and result.created_time == result.last_updated_time])
    update_count = sum([1 for result in created if result.was_modified and result.created_time != result.last_updated_time])
    unchanged_count = len(created) - create_count - update_count
    logger.info(f"Created {create_count} updated {update_count}, and {unchanged_count} unchanged annotations for {result.job_id}")
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