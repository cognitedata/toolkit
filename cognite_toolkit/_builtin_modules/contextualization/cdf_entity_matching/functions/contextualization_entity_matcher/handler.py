import itertools
import traceback
from collections.abc import Iterable, MutableSequence
from typing import Any, Literal

from cognite.client.config import global_config

from my_dev.function_local_venvs.contextualization_p_and_id_annotater.local_code.handler import wait_for_completion

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
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel

FUNCTION_ID = "contextualization_entity_matcher"
EXTRACTION_PIPELINE_EXTERNAL_ID = "ctx_entity_matching"
EXTERNAL_ID_LIMIT = 256
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
            error_msg = error_msg[: EXTRACTION_RUN_MESSAGE_LIMIT - len(prefix) - len(suffix) - 3]
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

    def _lookup_cursor(self, key: str) -> str | None:
        row = self._client.raw.rows.retrieve(db_name=self._raw_database, table_name=self._raw_table, key=key)
        if row is None:
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
    properties: dict[str, str]
    name_by_alias: dict[str, str]

    @classmethod
    def from_node(cls, node: dm.Node, properties: list[str]) -> "Entity":
        if not node.properties:
            raise ValueError(f"Node {node.as_id()} does not have properties")
        view_id, node_properties = next(iter(node.properties.items()))
        properties: dict[str, str] = {}
        name_by_alias: dict[str, str] = {}
        for no, prop in enumerate(properties):
            if prop in node_properties:
                # We standardize the property names to prop0, prop1, prop2, ...
                # This is such that we can easily match the properties to multiple target entities
                alias = f"prop{no}"
                properties[alias] = node_properties[prop]
                name_by_alias[alias] = prop
        return cls(
            node_id=node.as_id(),
            view=view_id,
            properties=properties,
            name_by_alias=name_by_alias,
        )

    @classmethod
    def from_annotation(cls, data: dict[str, Any]) -> "list[Entity]":
        return [cls.model_validate(item) for item in data["entities"]]

    def dump(self) -> dict[str, Any]:
        return {
            "nodeId": self.node_id.dump(),
            "view": self.view.dump(),
            "properties": self.properties,
            "nameByAlias": self.name_by_alias,
        }


class EntityList(list, MutableSequence[Entity]):
    @property
    def unique_properties(self) -> set[str]:
        return set().union(*[entity.properties.keys() for entity in self])

    @classmethod
    def from_nodes(cls, nodes: list[dm.Node], properties: list[str]) -> "EntityList":
        return cls([Entity.from_node(node, properties) for node in nodes])

    def dump(self) -> list[dict[str, Any]]:
        return [entity.dump() for entity in self]

    def property_product(self, other: "EntityList") -> list[dict[str, str]]:
        return [
            {
                "source": source,
                "target": target,
            }
            for source, target in itertools.product(self.unique_properties, other.unique_properties)
        ]


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

    logger.info(f"Matching jobs triggered: {len(job_by_name)}")

    annotation_count = 0
    for result in wait_for_completion(job_by_name, logger):
        if result.errors:
            errors_str = "\n  - ".join(sorted(set(result.errors)))
            logger.error(f"Job {result.job_id} {len(result.errors)} matching failed: \n  - {errors_str}")
            continue
        annotations = write_annotations(
            result, client, config.data.annotation_space, config.source_system, config.parameters, logger
        )
        annotation_count += len(annotations)
    logger.info(f"Annotations created: {annotation_count}")


def trigger_matching_jobs(
    client: CogniteClient, cursors: Cursors, config: Config, logger: CogniteFunctionLogger
) -> list[ContextualizationJob]:
    instance_spaces = config.data.instance_spaces
    jobs: list = []

    for job_name, job_config in config.data.matching_jobs.items():
        last_cursor = cursors.get_cursor(job_name)
        query = _create_query(job_config.source_view, instance_spaces, last_cursor)

        target_entities = EntityList()
        for target_view in job_config.target_views:
            target_nodes = client.data_modeling.instances.list(
                instance_type="node",
                sources=[target_view.as_view_id()],
                space=instance_spaces,
                limit=-1,
            )
            target_entities.extend(EntityList.from_nodes(target_nodes, target_view.properties))

        for source_nodes in client.data_modeling.instances.sync(query):
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
            jobs.append(job)
    return jobs


def _create_query(view: ViewProperties, instance_spaces: list[str], last_cursor: str | None) -> dm.query.Query:
    view_id = view.as_view_id()
    is_selected = dm.filters.And(
        dm.filters.In(["node", "space"], instance_spaces),
        dm.filters.HasData(views=[view_id]),
    )
    return dm.query.Query(
        with_={
            "entities": dm.query.NodeResultSetExpression(
                filter=is_selected,
                limit=1000,
            )
        },
        select={"entities": dm.query.Select([dm.query.SourceSelector(source=view_id, properties=view.properties)])},
        cursors={"entities": last_cursor},
    )


def wait_for_completion(jobs: list, logger: CogniteFunctionLogger) -> Iterable:
    raise NotImplementedError("Diagram detection is not yet implemented")


def write_annotations(
    result: Any,
    client: CogniteClient,
    annotation_space: str,
    source: dm.DirectRelationReference,
    parameter: Parameters,
    logger: CogniteFunctionLogger,
) -> list[CogniteAnnotationApply]:
    raise NotImplementedError("Diagram detection is not yet implemented")


def load_config(client: CogniteClient, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(EXTRACTION_PIPELINE_EXTERNAL_ID)
    if raw_config.config is None:
        raise ValueError("No config found for extraction pipeline")
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
