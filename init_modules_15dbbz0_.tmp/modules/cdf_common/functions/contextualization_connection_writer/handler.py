import json
import sys
import traceback
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import ClassVar, Literal, TypeVar, cast

from cognite.client.config import global_config
from cognite.client.exceptions import CogniteAPIError

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
# ruff: noqa: E402
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True
import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRunWrite, RowWrite
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDiagramAnnotation, CogniteDiagramAnnotationApply
from pydantic import BaseModel, model_validator
from pydantic.alias_generators import to_camel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


FUNCTION_ID = "connection_writer"
EXTRACTION_PIPELINE_EXTERNAL_ID = "ctx_files_direct_relation_write"
EXTERNAL_ID_LIMIT = 256
EXTRACTION_RUN_MESSAGE_LIMIT = 1000


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        connection_count = execute(data, client)
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
        message = f"{FUNCTION_ID} executed successfully. Created {connection_count} connections"

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


class ViewProperty(BaseModel, alias_generator=to_camel):
    space: str
    external_id: str
    version: str
    direct_relation_property: str | None = None

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class DirectRelationMapping(BaseModel, alias_generator=to_camel):
    start_node_view: ViewProperty
    end_node_view: ViewProperty

    @model_validator(mode="after")
    def direct_relation_is_set(self) -> Self:
        if (
            sum(
                1
                for prop in (self.start_node_view.direct_relation_property, self.end_node_view.direct_relation_property)
                if prop is not None
            )
            != 1
        ):
            raise ValueError("You must set 'directRelationProperty' for at either of 'startNode' or 'endNode'")
        return self


class ConfigData(BaseModel, alias_generator=to_camel):
    annotation_space: str
    direct_relation_mappings: list[DirectRelationMapping]


class ConfigState(BaseModel, alias_generator=to_camel):
    raw_database: str
    raw_table: str


class Config(BaseModel, alias_generator=to_camel):
    data: ConfigData
    state: ConfigState


class State(BaseModel):
    key: ClassVar[str] = FUNCTION_ID
    last_cursor: str | None = None

    @classmethod
    def from_cdf(cls, client: CogniteClient, state: ConfigState) -> "State":
        row = client.raw.rows.retrieve(db_name=state.raw_database, table_name=state.raw_table, key=cls.key)
        if row is None:
            return cls()
        return cls.model_validate(row.columns)

    def to_cdf(self, client: CogniteClient, state: ConfigState) -> None:
        client.raw.rows.insert(
            db_name=state.raw_database,
            table_name=state.raw_table,
            row=self._as_row(),
        )

    def _as_row(self) -> RowWrite:
        return RowWrite(
            key=self.key,
            columns=self.model_dump(),
        )


################# Functions #################


def execute(data: dict, client: CogniteClient) -> int:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO"))  # type: ignore[arg-type]
    logger.debug("Starting connection write")
    config = load_config(client, logger)
    logger.debug("Loaded config successfully")

    state = State.from_cdf(client, config.state)
    connection_count = 0
    for annotation_list in iterate_new_approved_annotations(state, client, config.data.annotation_space, logger):
        annotation_by_source_by_node = to_direct_relations_by_source_by_node(
            annotation_list, config.data.direct_relation_mappings, logger
        )
        connections = write_connections(annotation_by_source_by_node, client, logger)
        connection_count += connections

    state.to_cdf(client, config.state)
    logger.info(f"Created {connection_count} connections")
    return connection_count


T = TypeVar("T")


def chunker(items: Sequence[T], chunk_size: int) -> Iterable[Sequence[T]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def iterate_new_approved_annotations(
    state: State, client: CogniteClient, annotation_space: str, logger: CogniteFunctionLogger, chunk_size: int = 1000
) -> Iterable[list[CogniteDiagramAnnotation]]:
    query = create_query(state.last_cursor, annotation_space)
    try:
        result = client.data_modeling.instances.sync(query)
    except CogniteAPIError as e:
        if e.code == 400 and "Cursor has expired" in e.message:
            logger.warning("Cursor has expired, starting from the beginning")
            state.last_cursor = None
            query = create_query(state.last_cursor, annotation_space)
            result = client.data_modeling.instances.sync(query)
        else:
            raise
    edges = result["annotations"]
    logger.debug(f"Retrieved {len(edges)} new approved annotations")
    state.last_cursor = edges.cursor
    for edge_list in chunker(list(edges), chunk_size):
        yield [CogniteDiagramAnnotation._load(edge.dump()) for edge in edge_list]


def write_connections(
    annotation_by_source_by_property_by_view: dict[
        dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]
    ],
    client: CogniteClient,
    logger: CogniteFunctionLogger,
) -> int:
    connection_count = 0
    updated_nodes: list[dm.NodeApply] = []
    for view_id, annotation_by_source_by_property in annotation_by_source_by_property_by_view.items():
        node_ids = [node_id for node_id, _ in annotation_by_source_by_property.keys()]
        existing_node_list = client.data_modeling.instances.retrieve(node_ids, sources=[view_id]).nodes
        existing_node_by_id = {node.as_id(): node.as_write() for node in existing_node_list}

        for (node_id, direct_relation_property), direct_relation_ids in annotation_by_source_by_property.items():
            existing_node = existing_node_by_id.get(node_id)
            if existing_node is None:
                logger.warning(f"Node {node_id} not found in view {view_id}")
                continue
            for entity_source in existing_node.sources:
                if entity_source.source == view_id:
                    existing_connections = cast(list[dict], entity_source.properties.get(direct_relation_property, []))
                    before = len(existing_connections)
                    all_connections = {
                        dm.DirectRelationReference.load(connection) for connection in existing_connections
                    } | set(direct_relation_ids)
                    after = len(all_connections)
                    entity_source.properties[direct_relation_property] = [  # type: ignore[index]
                        connection.dump() for connection in all_connections
                    ]
                    connection_count += after - before
                    break
            updated_nodes.append(existing_node)

    updated = client.data_modeling.instances.apply(updated_nodes)
    logger.debug(f"Updated {len(updated.nodes)} nodes")
    return connection_count


def to_direct_relations_by_source_by_node(
    annotations: list[CogniteDiagramAnnotation], mappings: list[DirectRelationMapping], logger: CogniteFunctionLogger
) -> dict[dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]]:
    mapping_by_entity_source: dict[tuple[dm.ViewId, dm.ViewId], DirectRelationMapping] = {
        (mapping.start_node_view.as_view_id(), mapping.end_node_view.as_view_id()): mapping for mapping in mappings
    }
    annotation_by_source_by_node: dict[dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for annotation in annotations:
        try:
            source_context = json.loads(annotation.source_context)
        except json.JSONDecodeError:
            logger.error(f"Could not parse source context for annotation {annotation.external_id}")
            continue
        try:
            start_view = dm.ViewId.load(source_context["start"])
            end_view = dm.ViewId.load(source_context["end"])
        except KeyError:
            logger.error(f"Missing start or end in source context for annotation {annotation.external_id}")
            continue
        mapping = mapping_by_entity_source.get((start_view, end_view))
        if mapping is None:
            logger.warning(
                f"No mapping found for entity source {(start_view, end_view)} for annotation {annotation.external_id}"
            )
            continue
        if mapping.start_node_view.direct_relation_property is not None:
            update_node = annotation.start_node
            direct_relation_property = mapping.start_node_view.direct_relation_property
            other_side = annotation.end_node
            view_id = mapping.start_node_view.as_view_id()
        elif mapping.end_node_view.direct_relation_property is not None:
            update_node = annotation.end_node
            direct_relation_property = mapping.end_node_view.direct_relation_property
            other_side = annotation.start_node
            view_id = mapping.end_node_view.as_view_id()
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
                limit=1000,
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
    if raw_config.config is None:
        raise ValueError(f"Config for extraction pipeline {EXTRACTION_PIPELINE_EXTERNAL_ID} is empty")
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e
