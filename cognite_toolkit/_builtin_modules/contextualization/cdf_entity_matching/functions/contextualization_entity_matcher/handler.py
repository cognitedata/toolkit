import itertools
import json
import time
import traceback
from collections.abc import Iterable, MutableSequence, Sequence
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Literal, cast

from cognite.client.config import global_config

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
# ruff: noqa: E402
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True
import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite
from cognite.client.data_classes.contextualization import ContextualizationJob
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAnnotationApply
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

FUNCTION_ID = "contextualization_entity_matcher"
EXTRACTION_PIPELINE_EXTERNAL_ID = "ctx_entity_matching"
EXTERNAL_ID_LIMIT = 256
EXTRACTION_RUN_MESSAGE_LIMIT = 1000
EDGE_TYPE = "entity.match"


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
            error_msg = error_msg[: EXTRACTION_RUN_MESSAGE_LIMIT - len(prefix) - len(suffix) - 3 - 1]
            message = prefix + error_msg + '..."' + suffix
    else:
        status = "success"
        message = FUNCTION_ID

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRunWrite(extpipe_external_id=EXTRACTION_PIPELINE_EXTERNAL_ID, status=status, message=message)
    )
    return {"status": status, "message": message}


################# Data Classes #################


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    auto_reject_threshold: float = Field(gt=0.0, le=1.0)
    feature_type: Literal[
        "simple", "insensitive", "bigram", "frequencyweightedbigram", "bigramextratokenizers", "bigramcombo"
    ]


class ViewProperties(BaseModel, alias_generator=to_camel):
    space: str
    external_id: str
    version: str
    properties: list[str]

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class MatchingJobConfig(BaseModel, alias_generator=to_camel):
    source_view: ViewProperties
    target_views: list[ViewProperties]


class ConfigData(BaseModel, alias_generator=to_camel):
    instance_spaces: list[str]
    annotation_space: str
    matching_jobs: dict[str, MatchingJobConfig]


class ConfigState(BaseModel, alias_generator=to_camel):
    raw_database: str
    raw_table: str


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData
    source_system: dm.DirectRelationReference
    state: ConfigState

    @classmethod
    @field_validator("source_system", mode="before")
    def pares_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


class Cursors:
    def __init__(self, client: CogniteClient, state: ConfigState) -> None:
        self._cursor_by_key: dict[str, str | None] = {}
        self._client = client
        self._raw_database = state.raw_database
        self._raw_table = state.raw_table

    def get_cursor(self, key: str) -> str | None:
        if key not in self._cursor_by_key:
            self._cursor_by_key[key] = self._lookup_cursor(key)
        return self._cursor_by_key[key]

    def set_cursor(self, key: str, cursor: str | None) -> None:
        self._cursor_by_key[key] = cursor

    def _lookup_cursor(self, key: str) -> str | None:
        row = self._client.raw.rows.retrieve(db_name=self._raw_database, table_name=self._raw_table, key=key)
        if row is None or row.columns is None:
            return None
        return row.columns.get("cursor")

    def store(self, key: str) -> None:
        cursor = self.get_cursor(key)
        self._client.raw.rows.insert(
            db_name=self._raw_database,
            table_name=self._raw_table,
            row=RowWrite(key=key, columns={"cursor": cursor}),
        )


class Entity(BaseModel, alias_generator=to_camel, extra="allow", populate_by_name=True):
    node_id: dm.NodeId
    view: dm.ViewId
    standardized_properties: dict[str, str]
    name_by_alias: dict[str, str]

    @model_validator(mode="before")
    def pack_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "standardized_properties" in values or "standardizedProperties" in values:
            return values
        standardized_properties: dict[str, str] = {}
        for key in list(values.keys()):
            if key.startswith("prop"):
                standardized_properties[key] = values.pop(key)
        return {**values, "standardized_properties": standardized_properties}

    @field_validator("node_id", "view", "name_by_alias", mode="before")
    def load_json(cls, value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value

    @field_validator("node_id", mode="before")
    def load_node_id(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.NodeId.load(value)
        return value

    @field_validator("view", mode="before")
    def load_view_id(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.ViewId.load(value)
        return value

    @classmethod
    def from_node(cls, node: dm.Node, properties: list[str]) -> "Entity":
        if not node.properties:
            raise ValueError(f"Node {node.as_id()} does not have properties")
        view_id, node_properties = next(iter(node.properties.items()))
        standardized_properties: dict[str, str] = {}
        name_by_alias: dict[str, str] = {}
        for no, prop in enumerate(properties):
            if prop in node_properties:
                # We standardize the property names to prop0, prop1, prop2, ...
                # This is such that we can easily match the properties to multiple target entities
                alias = f"prop{no}"
                standardized_properties[alias] = str(node_properties[prop])
                name_by_alias[alias] = prop
        return cls(
            node_id=node.as_id(),
            view=view_id,
            standardized_properties=standardized_properties,
            name_by_alias=name_by_alias,
        )

    @classmethod
    def from_annotation(cls, data: dict[str, Any]) -> "list[Entity]":
        return [cls.model_validate(item) for item in data["entities"]]

    def dump(self) -> dict[str, Any]:
        return {
            "nodeId": json.dumps(self.node_id.dump()),
            "view": json.dumps(self.view.dump()),
            **self.standardized_properties,
            "nameByAlias": json.dumps(self.name_by_alias),
        }


class EntityList(list, MutableSequence[Entity]):
    @property
    def unique_properties(self) -> set[str]:
        return set().union(*[entity.standardized_properties.keys() for entity in self])

    @classmethod
    def from_nodes(cls, nodes: Sequence[dm.Node], properties: list[str]) -> "EntityList":
        return cls([Entity.from_node(node, properties) for node in nodes])

    def dump(self) -> list[dict[str, Any]]:
        return [entity.dump() for entity in self]

    def property_product(self, other: "EntityList") -> list[tuple[str, str]]:
        return [
            (source, target) for source, target in itertools.product(self.unique_properties, other.unique_properties)
        ]


class MatchItem(BaseModel, alias_generator=to_camel):
    score: float
    target: Entity


class MatchResult(BaseModel, alias_generator=to_camel):
    source: Entity
    matches: list[MatchItem]

    @property
    def best_match(self) -> MatchItem | None:
        return max(self.matches, key=lambda match: match.score) if self.matches else None

    @classmethod
    def load(cls, data: dict[str, Any]) -> "MatchResult":
        return cls.model_validate(data)


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

    def debug(self, message: str) -> None:
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)

    def info(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)

    def warning(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)

    def error(self, message: str) -> None:
        self._print("[ERROR]", message)


#####################################################

################# Functions #################


def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO"))  # type: ignore[arg-type]
    logger.debug("Starting entity matching")
    config = load_config(client, logger)
    logger.debug("Loaded config successfully")

    cursors = Cursors(client, config.state)
    job_by_name = trigger_matching_jobs(client, cursors, config, logger)
    job_name_by_id = {job.job_id: name for name, job in job_by_name.items()}
    jobs = list(job_by_name.values())
    logger.info(f"Matching jobs triggered: {len(job_by_name)}")

    annotation_count = 0
    for completed_job in wait_for_completion(jobs, logger):
        if completed_job.error_message:
            logger.error(f"Job {completed_job.job_id} entity matching failed: \n  - {completed_job.error_message}")
            continue
        annotation_list = create_annotations(
            completed_job, config.data.annotation_space, config.source_system, config.parameters, logger
        )
        write_annotations(annotation_list, client, completed_job.job_id or 0, logger)
        annotation_count += len(annotation_list)
        job_name = job_name_by_id[cast(int, completed_job.job_id)]
        cursors.store(job_name)

    logger.info(f"Annotations created: {annotation_count}")


def trigger_matching_jobs(
    client: CogniteClient, cursors: Cursors, config: Config, logger: CogniteFunctionLogger
) -> dict[str, ContextualizationJob]:
    instance_spaces = config.data.instance_spaces
    jobs: dict[str, ContextualizationJob] = {}

    for job_name, job_config in config.data.matching_jobs.items():
        target_entities = EntityList()
        for target_view in job_config.target_views:
            target_nodes = client.data_modeling.instances.list(
                instance_type="node",
                sources=[target_view.as_view_id()],
                space=instance_spaces,
                limit=-1,
            )
            target_entities.extend(EntityList.from_nodes(target_nodes, target_view.properties))

        last_cursor = cursors.get_cursor(job_name)

        query = _create_query(job_config.source_view, instance_spaces, last_cursor, job_name)
        query_result = client.data_modeling.instances.sync(query)
        cursors.set_cursor(job_name, query_result.cursors[job_name])
        source_nodes = query_result.get_nodes(job_name)

        source_entities = EntityList.from_nodes(source_nodes, job_config.source_view.properties)
        combinations = source_entities.property_product(target_entities)
        unsupervised_model = client.entity_matching.fit(
            sources=source_entities.dump(),
            targets=target_entities.dump(),
            feature_type=config.parameters.feature_type,
            match_fields=combinations,
        )
        job = unsupervised_model.predict(
            sources=source_entities.dump(),
            targets=target_entities.dump(),
            num_matches=1,
            score_threshold=config.parameters.auto_reject_threshold,
        )
        jobs[job_name] = job
        logger.debug(f"Triggered matching job {job_name} with {len(source_entities)} entities")
    return jobs


def _create_query(
    view: ViewProperties, instance_spaces: list[str], last_cursor: str | None, name: str
) -> dm.query.Query:
    view_id = view.as_view_id()
    is_selected = dm.filters.And(
        dm.filters.In(["node", "space"], instance_spaces),
        dm.filters.HasData(views=[view_id]),
    )
    return dm.query.Query(
        with_={
            name: dm.query.NodeResultSetExpression(
                filter=is_selected,
                limit=1000,
            )
        },
        select={name: dm.query.Select([dm.query.SourceSelector(source=view_id, properties=view.properties)])},
        cursors={name: last_cursor},
    )


def wait_for_completion(
    jobs: list[ContextualizationJob], logger: CogniteFunctionLogger
) -> Iterable[ContextualizationJob]:
    # The Cognite Function will eventually time out, so we don't need to worry about running forever
    while jobs:
        job = jobs.pop(0)

        job.update_status()

        status = cast(str, job.status).casefold()
        if status == "completed":
            yield job
        elif status in ("failed", "timeout"):
            logger.warning(f"Job {job.job_id} {status}: {job.error_message}")
        else:
            jobs.append(job)
            logger.debug(f"Job {job.job_id} {status}, will check again later")
            # Sleep for a bit to avoid hammering the API
            time.sleep(10)


def create_annotations(
    job: ContextualizationJob,
    annotation_space: str,
    source: dm.DirectRelationReference,
    parameters: Parameters,
    logger: CogniteFunctionLogger,
) -> list[CogniteAnnotationApply]:
    annotation_list: list[CogniteAnnotationApply] = []
    for match_raw in job.result["items"]:
        match = MatchResult.load(match_raw)
        if (best_match := match.best_match) is None:
            logger.debug(f"No match found for {match.source.node_id!r}")
            continue
        source_id = match.source.node_id
        target_id = best_match.target.node_id
        now = datetime.now(timezone.utc).replace(microsecond=0)
        score = best_match.score
        status: Literal["Approved", "Suggested", "Rejected"] = "Suggested"
        if score >= parameters.auto_approval_threshold:
            status = "Approved"
        elif score <= parameters.auto_reject_threshold:
            status = "Rejected"
        external_id = create_annotation_id(source_id, target_id, EDGE_TYPE)
        annotation = CogniteAnnotationApply(
            space=annotation_space,
            external_id=external_id,
            start_node=(source_id.space, source_id.external_id),
            end_node=(target_id.space, target_id.external_id),
            type=(annotation_space, EDGE_TYPE),
            confidence=score,
            status=status,
            source=source,
            source_created_time=now,
            source_updated_time=now,
            source_created_user=FUNCTION_ID,
            source_updated_user=FUNCTION_ID,
            source_context=json.dumps({"end": best_match.target.view.dump(), "start": match.source.view.dump()}),
        )
        annotation_list.append(annotation)
    return annotation_list


def write_annotations(
    annotation_list: list[CogniteAnnotationApply],
    client: CogniteClient,
    job_id: int,
    logger: CogniteFunctionLogger,
) -> None:
    created = client.data_modeling.instances.apply(edges=annotation_list).edges

    create_count = sum(
        [1 for result in created if result.was_modified and result.created_time == result.last_updated_time]
    )
    update_count = sum(
        [1 for result in created if result.was_modified and result.created_time != result.last_updated_time]
    )
    unchanged_count = len(created) - create_count - update_count
    logger.info(
        f"Created {create_count} updated {update_count}, and {unchanged_count} unchanged annotations for {job_id}"
    )


def create_annotation_id(start: dm.NodeId, end: dm.NodeId, type: str) -> str:
    naive = f"{start.space}:{start.external_id}:{end.space}:{end.external_id}:{type}"
    if len(naive) < EXTERNAL_ID_LIMIT:
        return naive
    full_hash = sha256(naive.encode()).hexdigest()[:10]
    prefix = f"{start.external_id}:{end.external_id}:{type}"
    shorten = f"{prefix}:{full_hash}"
    if len(shorten) < EXTERNAL_ID_LIMIT:
        return shorten
    return prefix[: EXTERNAL_ID_LIMIT - 10] + full_hash


def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    if raw_config.config is None:
        raise ValueError("No config found for extraction pipeline")
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
