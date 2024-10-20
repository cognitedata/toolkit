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

FUNCTION_ID = "p_and_id_connection_writer"
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


################# Functions #################

def execute(data: dict, client: CogniteClient) -> None:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO")) # type: ignore[arg-type]
    logger.debug("Starting connection write")

    connection_count = 0
    for annotation_list in iterate_new_approved_annotations(client, logger):
        connections = write_connections(annotation_list, client, logger)
        connection_count += connections

    logger.info(f"Created {connection_count} connections")


def iterate_new_approved_annotations(client: CogniteClient, logger: CogniteFunctionLogger) -> Iterable[list[CogniteDiagramAnnotationApply]]:
    raise NotImplementedError("This function is not implemented yet")

def write_connections(annotations: list[CogniteDiagramAnnotationApply], client: CogniteClient, logger: CogniteFunctionLogger) -> int:
    raise NotImplementedError("This function is not implemented yet")
