from __future__ import annotations

import os
import re
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Annotation,
    AnnotationFilter,
    AnnotationList,
    ExtractionPipelineRun,
    FileMetadata,
    FileMetadataUpdate,
)
from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.utils._text import shorten

# P&ID original file defaults
ORG_MIME_TYPE = "application/pdf"
FILE_ANNOTATED_METADATA_KEY = "FILE_ANNOTATED"
ANNOTATION_ERROR_MSG = "annotation_created_error"

# Annotation defaults
ASSET_ANNOTATION_TYPE = "diagrams.AssetLink"
FILE_ANNOTATION_TYPE = "diagrams.FileLink"
ANNOTATION_STATUS_APPROVED = "approved"
ANNOTATION_STATUS_SUGGESTED = "suggested"
ANNOTATION_RESOURCE_TYPE = "file"
CREATING_APP = "P&ID contextualization and annotation function"
CREATING_APPVERSION = "1.0.0"

# Asset constants
MAX_LENGTH_METADATA = 10000

# Other constants
FUNCTION_NAME = "P&ID Annotation"
ISO_8601 = "%Y-%m-%d %H:%M:%S"


@dataclass
class AnnotationConfig:
    extraction_pipeline_ext_id: str
    debug: bool
    run_all: bool
    doc_limit: int
    doc_data_set_ext_id: str
    doc_type_meta_col: str
    p_and_id_doc_type: str
    asset_root_ext_ids: list[str]
    match_threshold: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> AnnotationConfig:
        return cls(
            extraction_pipeline_ext_id=data["ExtractionPipelineExtId"],
            debug=data["debug"],
            run_all=data["runAll"],
            doc_limit=data["docLimit"],
            doc_data_set_ext_id=data["docDataSetExtId"],
            doc_type_meta_col=data["docTypeMetaCol"],
            p_and_id_doc_type=data["pAndIdDocType"],
            asset_root_ext_ids=data["assetRootExtIds"],
            match_threshold=data["matchThreshold"],
        )


@dataclass
class Entity:
    external_id: str
    org_name: str
    name: list[str]
    id: int
    type: str = "file"

    def dump(self) -> dict[str, Any]:
        return {
            "externalId": self.external_id,
            "orgName": self.org_name,
            "name": self.name,
            "id": self.id,
            "type": self.type,
        }


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    try:
        config = load_config_parameters(client, data)
        annotate_p_and_id(client, config)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {FUNCTION_NAME}: Extraction failed - Message: {e!r} - {tb}"
        print(f"[FAILED] Error: {msg}")
        return {
            "error": str(e),
            "status": "failed",
            "data": data,
            "secrets": mask_secrets(secrets),
            "functionInfo": function_call_info,
        }
    return {
        "status": "succeeded",
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}


def load_config_parameters(cognite_client: CogniteClient, function_data: dict[str, Any]) -> AnnotationConfig:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""

    try:
        extraction_pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    except KeyError:
        raise ValueError("Missing parameter 'ExtractionPipelineExtId' in function data")

    try:
        pipeline_config_str = cognite_client.extraction_pipelines.config.retrieve(extraction_pipeline_ext_id)
        if pipeline_config_str and pipeline_config_str != "":
            data = yaml.safe_load(pipeline_config_str.config)["data"]
        else:
            raise Exception("No configuration found in pipeline")
    except Exception as e:
        raise Exception(f"Not able to load pipeline : {extraction_pipeline_ext_id} configuration - {e}")

    data["ExtractionPipelineExtId"] = extraction_pipeline_ext_id
    return AnnotationConfig.load(data)


def annotate_p_and_id(cognite_client: CogniteClient, config: AnnotationConfig) -> None:
    """
    Read configuration and start P&ID annotation process by
    1. Reading files to annotate
    2. Get file entities to be matched aganst files in P&ID
    3. Read existing annotations for the found files
    4. Get assets and put it into the list of entities to be found in the P&ID
    5. Process file:
        - detecting entities
        - creation annotations.
        - remove duplicate annotations

    Args:
        cognite_client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process

    """

    print("Initiating Annotation[INFO]  of P&ID")

    for asset_root_ext_id in config.asset_root_ext_ids:
        try:
            all_files, filer_to_process = get_files(
                cognite_client,
                asset_root_ext_id,
                config,
            )
            entities = get_files_entities(all_files)

            if len(entities) > 0:
                annotation_list = get_existing_annotations(cognite_client, entities)
            else:
                annotation_list = {}

            annotated_count = 0
            error_count = 0
            if len(filer_to_process) > 0:
                append_asset_entities(entities, cognite_client, asset_root_ext_id)
                annotated_count, error_count = process_files(
                    cognite_client,
                    entities,
                    filer_to_process,
                    annotation_list,
                    config,
                )

            msg = (
                f"Annotated P&ID files for asset: {asset_root_ext_id} number of files annotated: {annotated_count}, "
                f"file not annotaded due to errors: {error_count}"
            )
            print(f"[INFO] {msg}")
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="success",
                    message=msg,
                )
            )

        except Exception as e:
            msg = (
                f"Annotated P&ID files failed on root asset: {asset_root_ext_id}. "
                f"Message: {e!s}, traceback:\n{traceback.format_exc()}"
            )
            print(f"[ERROR] {msg}")
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="failure",
                    message=shorten(msg, 1000),
                )
            )


def get_files(
    cognite_client: CogniteClient,
    asset_root_ext_id: str,
    config: AnnotationConfig,
) -> tuple[dict[str, FileMetadata], dict[str, FileMetadata]]:
    """
    Read files based on doc_type and mime_type to find P&ID files

    :returns: dict of files
    """

    p_and_id_files_all_by_ext_id: dict[str, FileMetadata] = {}  # Define a type for Dict
    p_and_id_file_to_process_by_ext_id: dict[str, FileMetadata] = {}  # Define a type for Dict
    doc_count = 0
    meta_file_update: list[FileMetadataUpdate] = []

    print(
        f"[INFO] Get files to annotate data set: {config.doc_data_set_ext_id}, asset root: {asset_root_ext_id} "
        f"doc_type: {config.p_and_id_doc_type} and mime_type: {ORG_MIME_TYPE}"
    )
    file_list = cognite_client.files.list(
        metadata={config.doc_type_meta_col: config.p_and_id_doc_type},
        data_set_external_ids=[config.doc_data_set_ext_id],
        asset_subtree_external_ids=[asset_root_ext_id],
        mime_type=ORG_MIME_TYPE,
        limit=config.doc_limit,
    )
    for file in file_list:
        doc_count += 1
        p_and_id_files_all_by_ext_id[file.external_id] = file

        if FILE_ANNOTATED_METADATA_KEY is not None and FILE_ANNOTATED_METADATA_KEY not in (file.metadata or {}):
            if file.external_id is not None:
                p_and_id_file_to_process_by_ext_id[file.external_id] = file

        # if run all - remove metadata element from last annotation
        elif config.run_all:
            if not config.debug and FILE_ANNOTATED_METADATA_KEY is not None:
                file_meta_update = FileMetadataUpdate(external_id=file.external_id).metadata.remove(
                    [FILE_ANNOTATED_METADATA_KEY]
                )
                meta_file_update.append(file_meta_update)
            if file.external_id is not None:
                p_and_id_file_to_process_by_ext_id[file.external_id] = file
        else:
            update_file_metadata(
                meta_file_update,
                file,
                p_and_id_file_to_process_by_ext_id,
            )
        if config.debug:
            break

    if len(meta_file_update) > 0:
        cognite_client.files.update(meta_file_update)

    return p_and_id_files_all_by_ext_id, p_and_id_file_to_process_by_ext_id


def update_file_metadata(
    meta_file_update: list[FileMetadataUpdate],
    file: FileMetadata,
    p_and_id_files: dict[str, FileMetadata],
) -> None:
    annotated_date = None
    if file.metadata and FILE_ANNOTATED_METADATA_KEY is not None:
        file_annotated_time = file.metadata.get(FILE_ANNOTATED_METADATA_KEY, None)
        if file_annotated_time:
            try:
                annotated_date = datetime.strptime(file_annotated_time, ISO_8601)
            except ValueError:
                raise ValueError(
                    f"Failed to parse date from metadata {FILE_ANNOTATED_METADATA_KEY}: {file_annotated_time}"
                )

    annotated_stamp = int(annotated_date.timestamp() * 1000) if annotated_date else None
    if (
        annotated_stamp is not None and file.last_updated_time is not None and file.last_updated_time > annotated_stamp
    ):  # live 1 h for buffer
        if FILE_ANNOTATED_METADATA_KEY is not None:  # Check for None
            file_meta_update = FileMetadataUpdate(external_id=file.external_id).metadata.remove(
                [FILE_ANNOTATED_METADATA_KEY]
            )
            meta_file_update.append(file_meta_update)
        if file.external_id is not None:
            p_and_id_files[file.external_id] = file


def get_files_entities(p_and_id_files: dict[str, FileMetadata]) -> list[Entity]:
    """
    Loop found P&ID files and create a list of entities used for matching against file names in P&ID

    Args:
        p_and_id_files: Dict of files found based on filter
    """
    entities: list[Entity] = []
    doc_count = 0

    for file_ext_id, file_meta in p_and_id_files.items():
        doc_count += 1
        fname_list = []
        if file_meta.name is None:
            print(f"[WARNING] No name found for file with external ID: {file_ext_id}, and metadata: {file_meta}")
            continue

        # build list with possible file name variations used in P&ID to refer to other P&ID
        split_name = re.split("[,._ \\-!?:]+", file_meta.name)

        core_name = ""
        next_name = ""
        for name in reversed(split_name):
            if core_name == "":
                idx = file_meta.name.find(name)
                core_name = file_meta.name[: idx - 1]
                fname_list.append(core_name)
            else:
                idx = core_name.find(name + next_name)
                if idx != 0:
                    ctx_name = core_name[idx:]
                    if next_name != "":  # Ignore first part of name in matching
                        fname_list.append(ctx_name)
                    next_name = core_name[idx - 1 :]

        # add entities for files used to match between file references in P&ID to other files
        entities.append(
            Entity(
                external_id=file_ext_id,
                org_name=file_meta.name,
                name=fname_list,
                id=file_meta.id,
                type="file",
            )
        )

    return entities


def get_existing_annotations(
    cognite_client: CogniteClient, entities: list[Entity]
) -> dict[Optional[int], list[Optional[int]]]:
    """
    Read list of already annotated files and get corresponding annotations

    :param cognite_client: Dict of files found based on filter
    :param entities:

    :returns: dictionary of annotations
    """

    annotation_list = AnnotationList([])
    annotated_file_text: dict[Optional[int], list[Optional[int]]] = defaultdict(list)

    print("Get existing[INFO]  annotations based on annotated_resource_type= file, and filtered by found files")
    file_list = [{"id": item.id} for item in entities]

    n = 1000
    for i in range(0, len(file_list), n):
        sub_file_list = file_list[i : i + n]

        if len(sub_file_list) > 0:
            filter_ = AnnotationFilter(annotated_resource_type="file", annotated_resource_ids=sub_file_list)
            annotation_list = cognite_client.annotations.list(limit=-1, filter=filter_)

        for annotation in annotation_list:
            annotation: Annotation
            # only get old annotations created by this app - do not touch manual or other created annotations
            if annotation.creating_app == CREATING_APP:
                annotated_file_text[annotation.annotated_resource_id].append(annotation.id)

    return annotated_file_text


def append_asset_entities(entities: list[Entity], cognite_client: CogniteClient, asset_root_ext_id: str) -> None:
    """Get Asset used as input to contextualization
    Args:
        cognite_client: Instance of CogniteClient
        asset_root_ext_id: external root asset ID
        entities: list of entites found so fare (file names)

    Returns:
        list of entities
    """

    print(f"[INFO] Get assets based on asset_subtree_external_ids = {asset_root_ext_id}")
    assets = cognite_client.assets.list(asset_subtree_external_ids=[asset_root_ext_id], limit=-1)

    # clean up dummy tags and system numbers
    for asset in assets:
        name = asset.name
        try:
            names = []
            not_dummy = True
            if (
                asset.metadata is not None
                and "Description" in asset.metadata
                and "DUMMY TAG" in asset.metadata.get("Description", "").upper()
            ):
                not_dummy = False

            if name is not None and len(name) > 3 and not_dummy:  # ignore system asset names (01, 02, ...)
                names.append(name)

                # Split name - and if a system number is used also add name without system number to list
                split_name = re.split("[,._ \\-:]+", name)
                if split_name[0].isnumeric():
                    names.append(name[len(split_name[0]) + 1 :])

                entities.append(
                    Entity(
                        external_id=asset.external_id,
                        org_name=name,
                        name=name,
                        id=asset.id,
                        type="asset",
                    )
                )
        except Exception:
            print(f"[ERROR] Not able to get entities for asset name: {name}, id {asset.external_id}")


def process_files(
    cognite_client: CogniteClient,
    entities: list[Entity],
    files: dict[str, FileMetadata],
    annotation_list: dict[Optional[int], list[Optional[int]]],
    config: AnnotationConfig,
) -> tuple[int, int]:
    """Contextualize files by calling the annotation function
    Then update the metadata for the P&ID input file

    Args:
        cognite_client: client id used to connect to CDF
        entities: list of input entities that are used to match content in file
        files: dict of files found based on filter
        annotation_list: list of existing annotations for input files
        config: configuration for the annotation process

    Returns:
        number of annotated files and number of errors
    """
    annotated_count = 0
    error_count = 0
    annotation_list = annotation_list or {}

    for file_ext_id, file in files.items():
        print(f"[INFO] Parse and annotate, input file: {file_ext_id}")

        try:
            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                cognite_client, config.match_threshold, file_ext_id, entities, annotation_list
            )

            # create a string of matched tag - to be added to metadata
            asset_names = ",".join(map(str, entities_name_found))
            if len(asset_names) > MAX_LENGTH_METADATA:
                asset_names = asset_names[0:MAX_LENGTH_METADATA] + "..."

            file_asset_ids = list(file.asset_ids) if file.asset_ids else []

            # merge existing assets with new-found, and create a list without duplicates
            asset_ids_list = list(set(file_asset_ids + entities_id_found))

            # If list of assets more than 1000 items, cut the list at 1000
            if len(asset_ids_list) > 1000:
                print(
                    f"[WARNING] List of assetsIds for file {file.external_id} > 1000 ({len(asset_ids_list)}), "
                    "cutting list at 1000 items"
                )
                asset_ids_list = asset_ids_list[:1000]

            if not config.debug:
                annotated_count += 1
                # Update metadata from found PDF files
                try:
                    # Note uses local time, since file update time also uses local time + add a minute
                    # making sure annotation time is larger than last update time
                    now = datetime.now() + timedelta(minutes=1)
                    timestamp = now.strftime(ISO_8601)
                    my_update = (
                        FileMetadataUpdate(id=file.id)
                        .asset_ids.set(asset_ids_list)
                        .metadata.add({FILE_ANNOTATED_METADATA_KEY: timestamp, "tags": asset_names})
                    )
                    safe_files_update(cognite_client, my_update, file.external_id)
                except Exception as e:
                    s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
                    print(f"[WARNING] Not able to update reference doc : {file_ext_id} - {s}  - {r}")
                    pass

            else:
                print(f"[INFO] Converted and created (not upload due to DEBUG) file: {file_ext_id}")
                print(f"[INFO] Assets found: {asset_names}")

        except Exception as e:
            error_count += 1
            s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
            msg = f"Failed to annotate the document, Message: {s}  - {r}"
            print(f"[ERROR] {msg}")
            if "KeyError" in r:
                timestamp = datetime.now(timezone.utc).strftime(ISO_8601)
                my_update = FileMetadataUpdate(id=file.id).metadata.add(
                    {FILE_ANNOTATED_METADATA_KEY: timestamp, ANNOTATION_ERROR_MSG: msg}
                )
                safe_files_update(cognite_client, my_update, file.external_id)

    return annotated_count, error_count


def detect_create_annotation(
    cognite_client: CogniteClient,
    match_threshold: float,
    file_ext_id: str,
    entities: list[Entity],
    annotation_list: dict[Optional[int], list[Optional[int]]],
) -> tuple[list[Any], list[Any]]:
    """
    Detect tags + files and create annotation for P&ID

    Args:
        cognite_client: client id used to connect to CDF
        match_threshold: score used to qualify match
        file_ext_id: file to be processed
        entities: list of input entities that are used to match content in file
        annotation_list: list of existing annotations for input files

    Returns:
        list of found entities and list of found entities ids

    """
    entities_id_found = []
    entities_name_found = []
    create_annotation_list: list[Annotation] = []
    to_delete_annotation_list: list[int] = []
    detected_count = 0

    # in case contextualization service not is available - back off and retry
    job = retrieve_diagram_with_retry(cognite_client, entities, file_ext_id)

    if "items" in job.result and len(job.result["items"]) > 0:
        # build a list of annotation BEFORE filtering on matchThreshold
        annotated_resource_id = job.result["items"][0]["fileId"]
        if annotated_resource_id in annotation_list:
            to_delete_annotation_list.extend(annotation_list[annotated_resource_id])

        detected_sytem_num, detected_count = get_sys_nums(job.result["items"][0]["annotations"], detected_count)
        for item in job.result["items"][0]["annotations"]:
            if item["entities"][0]["type"] == "file":
                annotation_type = FILE_ANNOTATION_TYPE
                ref_type = "fileRef"
                txt_value = item["entities"][0]["orgName"]
            else:
                annotation_type = ASSET_ANNOTATION_TYPE
                ref_type = "assetRef"
                txt_value = item["entities"][0]["orgName"]

            # logic to create suggestions for annotations if system number is missing from tag in P&ID
            # but a suggestion matches the most frequent system number from P&ID
            tokens = item["text"].split("-")
            if len(tokens) == 2 and item["confidence"] >= match_threshold and len(item["entities"]) == 1:
                sys_token_found = item["entities"][0]["name"][0].split("-")
                if len(sys_token_found) == 3:
                    sys_num_found = sys_token_found[0]
                    # if missing system number is in > 30% of the tag assume that it's correct -
                    # else create a suggestion
                    if sys_num_found in detected_sytem_num and detected_sytem_num[sys_num_found] / detected_count > 0.3:
                        annotation_status = ANNOTATION_STATUS_APPROVED
                    else:
                        annotation_status = ANNOTATION_STATUS_SUGGESTED
                else:
                    continue

            elif item["confidence"] >= match_threshold and len(item["entities"]) == 1:
                annotation_status = ANNOTATION_STATUS_APPROVED

            # If there are long asset names a lower confidence is ok to create a suggestion
            elif item["confidence"] >= 0.5 and item["entities"][0]["type"] == "asset" and len(tokens) > 5:
                annotation_status = ANNOTATION_STATUS_SUGGESTED
            else:
                continue

            if annotation_status == ANNOTATION_STATUS_APPROVED and annotation_type == ASSET_ANNOTATION_TYPE:
                entities_name_found.append(item["entities"][0]["orgName"])
                entities_id_found.append(item["entities"][0]["id"])

            x_min, x_max, y_min, y_max = get_coordinates(item["region"]["vertices"])

            annotation_data = {
                ref_type: {"id": item["entities"][0]["id"]},
                "pageNumber": item["region"]["page"],
                "text": txt_value,
                "textRegion": {
                    "xMax": x_max,
                    "xMin": x_min,
                    "yMax": y_max,
                    "yMin": y_min,
                },
            }

            file_annotation = Annotation(
                annotation_type=annotation_type,
                data=annotation_data,
                status=annotation_status,
                annotated_resource_type=ANNOTATION_RESOURCE_TYPE,
                annotated_resource_id=annotated_resource_id,
                creating_app=CREATING_APP,
                creating_app_version=CREATING_APPVERSION,
                creating_user=f"job.{job.job_id}",
            )

            create_annotation_list.append(file_annotation)

            # can only create 1000 annotations at a time.
            if len(create_annotation_list) >= 999:
                cognite_client.annotations.create(create_annotation_list)
                create_annotation_list = []

        if len(create_annotation_list) > 0:
            cognite_client.annotations.create(create_annotation_list)

        safe_delete_annotations(to_delete_annotation_list, cognite_client)

        # sort / deduplicate list of names and id
        entities_name_found = list(dict.fromkeys(entities_name_found))
        entities_id_found = list(dict.fromkeys(entities_id_found))

    return entities_name_found, entities_id_found


def retrieve_diagram_with_retry(
    cognite_client: CogniteClient, entities: list[Entity], file_id: str, retries: int = 3
) -> DiagramDetectResults:
    retry_num = 0
    while retry_num < retries:
        try:
            job = cognite_client.diagrams.detect(
                file_external_ids=[file_id],
                search_field="name",
                entities=[e.dump() for e in entities],
                partial_match=True,
                min_tokens=2,
            )
            return job
        except Exception as e:
            s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
            # retry func if CDF api returns an error
            if retry_num < 3:
                retry_num += 1
                print(f"[WARNING] Retry #{retry_num} - wait before retry - error was: {s}  - {r}")
                time.sleep(retry_num * 5)
            else:
                msg = f"Failed to detect entities, Message: {s}  - {r}"
                print(f"[ERROR] {msg}")
                raise Exception(msg)
    raise Exception("Failed to detect entities - max retries reached")


def get_sys_nums(annotations: Any, detected_count: int) -> tuple[dict[str, int], int]:
    """Get dict of used system number in P&ID. The dict is used to annotate if system
    number is missing - but then only annotation of found text is part of most
    frequent used system number

    Args:
        annotations: list of annotations found by context api
        detected_count: total number of detected system numbers

    Returns:
        dict of system numbers and number of times used
    """

    detected_sytem_num = {}

    for item in annotations:
        tokens = item["text"].split("-")
        if len(tokens) == 3:
            sys_num = tokens[0]
            detected_count += 1
            if sys_num in detected_sytem_num:
                detected_sytem_num[sys_num] += 1
            else:
                detected_sytem_num[sys_num] = 1

    return detected_sytem_num, detected_count


def get_coordinates(vertices: dict) -> tuple[int, int, int, int]:
    """Get coordinates for text box based on input from contextualization
    and convert it to coordinates used in annotations.

    :param vertices coordinates from contextualization

    :returns: coordinates used by annotations.
    """

    init_values = True
    x_max = 0
    x_min = 0
    y_max = 0
    y_min = 0

    for vert in vertices:
        # Values must be between 0 and 1
        x = 1 if vert["x"] > 1 else vert["x"]
        y = 1 if vert["y"] > 1 else vert["y"]

        if init_values:
            x_max = x
            x_min = x
            y_max = y
            y_min = y
            init_values = False
        else:
            if x > x_max:
                x_max = x
            elif x < x_min:
                x_min = x
            if y > y_max:
                y_max = y
            elif y < y_min:
                y_min = y

        if x_min == x_max:
            if x_min > 0.001:
                x_min -= 0.001
            else:
                x_max += 0.001

        if y_min == y_max:
            if y_min > 0.001:
                y_min -= 0.001
            else:
                y_max += 0.001

    return x_min, x_max, y_min, y_max


def safe_delete_annotations(delete_annotation_list: list[int], cognite_client: CogniteClient) -> None:
    """
    Clean up / delete exising annotations

    Handles any exception and log error if delete fails

    Args:

        delete_annotation_list: list of annotation IDs to be deleted
        cognite_client: Dict of files found based on filter
    """
    if len(delete_annotation_list) == 0:
        return
    unique_annotations = list(set(delete_annotation_list))
    try:
        cognite_client.annotations.delete(unique_annotations)
    except Exception as e:
        s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
        msg = f"Failed to delete annotations, Message: {s}  - {r}"
        print(f"[WARNING] {msg}")


def safe_files_update(
    cognite_client: CogniteClient,
    my_updates: FileMetadataUpdate | list[FileMetadataUpdate],
    file_ext_id: str,
) -> None:
    """
    Update metadata of original pdf files wit list of tags

    Catch exception and log error if update fails

    Args:
        cognite_client: client id used to connect to CDF
        my_updates: list of updates to be done
        file_ext_id: file to be updated
    """

    try:
        # write updates for existing files
        cognite_client.files.update(my_updates)
    except Exception as e:
        s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
        print(f"[ERROR] Failed to update the file {file_ext_id}, Message: {s}  - {r}")


def main():
    """
    Code used for local Test & Debug
    update local .env file to set variables to connect to CDF
    """

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    token_uri = os.environ["IDP_TOKEN_URL"]

    base_url = f"https://{cdf_cluster}.cognitedata.com"
    scopes = f"{base_url}/.default"
    secrets = {"mySecrets": "Values"}
    function_call_info = {"Debugging": "Called from Function main "}

    oauth_provider = OAuthClientCredentials(
        token_url=token_uri,
        client_id=client_id,
        client_secret=client_secret,
        scopes=[scopes],
    )

    cnf = ClientConfig(
        client_name=cdf_project_name,
        base_url=base_url,
        project=cdf_project_name,
        credentials=oauth_provider,
    )

    client = CogniteClient(cnf)

    data = {
        "ExtractionPipelineExtId": "ep_ctx_files_oid_fileshare_pandid_annotation",
    }

    # Test function handler
    handle(data, client, secrets, function_call_info)


if __name__ == "__main__":
    main()
