from __future__ import annotations

import os
import re
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

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

from .config import AnnotationConfig, load_config_parameters

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

# Other constants
ASSET_MAX_LEN_META = 10000
ISO_8601 = "%Y-%m-%d %H:%M:%S"


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


def handle(data: dict, client: CogniteClient) -> dict:
    config = load_config_parameters(client, data)
    annotate_pnid(client, config)
    return {"status": "succeeded", "data": data}


def annotate_pnid(client: CogniteClient, config: AnnotationConfig) -> None:
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
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """
    for asset_root_xid in config.asset_root_xids:
        try:
            all_files, filer_to_process = get_files(
                client,
                asset_root_xid,
                config,
            )
            entities = get_files_entities(all_files)

            if len(entities) > 0:
                annotation_list = get_existing_annotations(client, entities)
            else:
                annotation_list = {}

            annotated_count = 0
            error_count = 0
            if len(filer_to_process) > 0:
                append_asset_entities(entities, client, asset_root_xid)
                annotated_count, error_count = process_files(
                    client,
                    entities,
                    filer_to_process,
                    annotation_list,
                    config,
                )
            msg = (
                f"Annotated P&ID files for asset: {asset_root_xid} number of files annotated: {annotated_count}, "
                f"file not annotaded due to errors: {error_count}"
            )
            print(f"[INFO] {msg}")
            client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extpipe_xid,
                    status="success",
                    message=msg,
                )
            )

        except Exception as e:
            msg = (
                f"Annotated P&ID files failed on root asset: {asset_root_xid}. "
                f"Message: {e!s}, traceback:\n{traceback.format_exc()}"
            )
            print(f"[ERROR] {msg}")
            client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extpipe_xid,
                    status="failure",
                    message=shorten(msg, 1000),
                )
            )


def get_files(
    client: CogniteClient,
    asset_root_xid: str,
    config: AnnotationConfig,
) -> tuple[dict[str, FileMetadata], dict[str, FileMetadata]]:
    """
    Read files based on doc_type and mime_type to find P&ID files

    :returns: dict of files
    """
    doc_count = 0
    all_pnid_files: dict[str, FileMetadata] = {}
    pnids_to_process: dict[str, FileMetadata] = {}
    meta_file_update: list[FileMetadataUpdate] = []
    print(
        f"[INFO] Get files to annotate data set: {config.doc_data_set_xid}, asset root: {asset_root_xid} "
        f"doc_type: {config.pnid_doc_type} and mime_type: {ORG_MIME_TYPE}"
    )
    file_list = client.files.list(
        metadata={config.doc_type_meta_col: config.pnid_doc_type},
        data_set_external_ids=[config.doc_data_set_xid],
        asset_subtree_external_ids=[asset_root_xid],
        mime_type=ORG_MIME_TYPE,
        limit=config.doc_limit,
    )
    for file in file_list:
        doc_count += 1
        all_pnid_files[file.external_id] = file

        if FILE_ANNOTATED_METADATA_KEY is not None and FILE_ANNOTATED_METADATA_KEY not in (file.metadata or {}):
            if file.external_id is not None:
                pnids_to_process[file.external_id] = file

        # if run all - remove metadata element from last annotation
        elif config.run_all:
            if not config.debug and FILE_ANNOTATED_METADATA_KEY is not None:
                file_meta_update = FileMetadataUpdate(external_id=file.external_id).metadata.remove(
                    [FILE_ANNOTATED_METADATA_KEY]
                )
                meta_file_update.append(file_meta_update)
            if file.external_id is not None:
                pnids_to_process[file.external_id] = file
        else:
            update_file_metadata(
                meta_file_update,
                file,
                pnids_to_process,
            )
        if config.debug:
            break

    if len(meta_file_update) > 0:
        client.files.update(meta_file_update)

    return all_pnid_files, pnids_to_process


def update_file_metadata(
    meta_file_update: list[FileMetadataUpdate],
    file: FileMetadata,
    pnid_files: dict[str, FileMetadata],
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
            pnid_files[file.external_id] = file


def get_files_entities(pnid_files: dict[str, FileMetadata]) -> list[Entity]:
    """
    Loop found P&ID files and create a list of entities used for matching against file names in P&ID

    Args:
        pnid_files: Dict of files found based on filter
    """
    entities: list[Entity] = []
    doc_count = 0

    for file_xid, file_meta in pnid_files.items():
        doc_count += 1
        fname_list = []
        if file_meta.name is None:
            print(f"[WARNING] No name found for file with external ID: {file_xid}, and metadata: {file_meta}")
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
            Entity(external_id=file_xid, org_name=file_meta.name, name=fname_list, id=file_meta.id, type="file")
        )
    return entities


def get_existing_annotations(client: CogniteClient, entities: list[Entity]) -> dict[Optional[int], list[Optional[int]]]:
    """
    Read list of already annotated files and get corresponding annotations

    :param client: Dict of files found based on filter
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
            annotation_list = client.annotations.list(limit=-1, filter=filter_)

        for annotation in annotation_list:
            annotation: Annotation
            # only get old annotations created by this app - do not touch manual or other created annotations
            if annotation.creating_app == CREATING_APP:
                annotated_file_text[annotation.annotated_resource_id].append(annotation.id)

    return annotated_file_text


def append_asset_entities(entities: list[Entity], client: CogniteClient, asset_root_xid: str) -> None:
    """Get Asset used as input to contextualization
    Args:
        client: Instance of CogniteClient
        asset_root_xid: external root asset ID
        entities: list of entites found so fare (file names)

    Returns:
        list of entities
    """
    print(f"[INFO] Get assets based on asset_subtree_external_ids = {asset_root_xid}")
    assets = client.assets.list(asset_subtree_external_ids=[asset_root_xid], limit=-1)

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
    client: CogniteClient,
    entities: list[Entity],
    files: dict[str, FileMetadata],
    annotation_list: dict[Optional[int], list[Optional[int]]],
    config: AnnotationConfig,
) -> tuple[int, int]:
    """Contextualize files by calling the annotation function
    Then update the metadata for the P&ID input file

    Args:
        client: client id used to connect to CDF
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

    for file_xid, file in files.items():
        print(f"[INFO] Parse and annotate, input file: {file_xid}")

        try:
            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                client, config.match_threshold, file_xid, entities, annotation_list
            )

            # create a string of matched tag - to be added to metadata
            asset_names = shorten(",".join(map(str, entities_name_found)), ASSET_MAX_LEN_META)

            file_asset_ids = file.asset_ids or []

            # merge existing assets with new-found, and create a list without duplicates
            asset_ids_list = list(set(file_asset_ids + entities_id_found))

            # If list of assets more than 1000 items, cut the list at 1000
            if len(asset_ids_list) > 1000:
                print(
                    f"[WARNING] List of assetsIds for file {file.external_id} > 1000 ({len(asset_ids_list)}), "
                    "cutting list at 1000 items"
                )
                asset_ids_list = asset_ids_list[:1000]

            if config.debug:
                print(f"[INFO] Converted and created (not upload due to DEBUG) file: {file_xid}")
                print(f"[INFO] Assets found: {asset_names}")
            else:
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
                    safe_files_update(client, my_update, file.external_id)
                except Exception as e:
                    s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
                    print(f"[WARNING] Not able to update reference doc : {file_xid} - {s}  - {r}")

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
                safe_files_update(client, my_update, file.external_id)

    return annotated_count, error_count


def detect_create_annotation(
    client: CogniteClient,
    match_threshold: float,
    file_xid: str,
    entities: list[Entity],
    annotation_list: dict[Optional[int], list[Optional[int]]],
) -> tuple[list[Any], list[Any]]:
    """
    Detect tags + files and create annotation for P&ID

    Args:
        client: client id used to connect to CDF
        match_threshold: score used to qualify match
        file_xid: file to be processed
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
    job = retrieve_diagram_with_retry(client, entities, file_xid)

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
                client.annotations.create(create_annotation_list)
                create_annotation_list = []

        if len(create_annotation_list) > 0:
            client.annotations.create(create_annotation_list)

        safe_delete_annotations(to_delete_annotation_list, client)

        # sort / deduplicate list of names and id
        entities_name_found = list(dict.fromkeys(entities_name_found))
        entities_id_found = list(dict.fromkeys(entities_id_found))

    return entities_name_found, entities_id_found


def retrieve_diagram_with_retry(
    client: CogniteClient, entities: list[Entity], file_id: str, retries: int = 3
) -> DiagramDetectResults:
    retry_num = 0
    while retry_num < retries:
        try:
            job = client.diagrams.detect(
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

    Args:
        vertices (dict): coordinates from contextualization

    Returns:
        tuple[int, int, int, int]: coordinates used by annotations.
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


def safe_delete_annotations(delete_annotation_list: list[int], client: CogniteClient) -> None:
    """
    Clean up / delete exising annotations

    Handles any exception and log error if delete fails

    Args:
        delete_annotation_list: list of annotation IDs to be deleted
        client: Dict of files found based on filter
    """
    if not delete_annotation_list:
        return
    try:
        client.annotations.delete(list(set(delete_annotation_list)))
    except Exception as e:
        print(f"[ERROR] Failed to delete annotations, error: {type(e)}({e})")


def safe_files_update(
    client: CogniteClient,
    file_update: FileMetadataUpdate,
    file_xid: str,
) -> None:
    """
    Update metadata of original pdf file with list of tags

    Catch exception and log error if update fails

    Args:
        client: client id used to connect to CDF
        file_update: list of updates to be done
        file_xid: file to be updated
    """
    try:
        client.files.update(file_update)
    except Exception as e:
        print(f"[ERROR] Failed to update the file {file_xid!r}, error: {type(e)}({e})")


def run_locally():
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

    client = CogniteClient(
        ClientConfig(
            client_name=cdf_project_name,
            base_url=base_url,
            project=cdf_project_name,
            credentials=OAuthClientCredentials(
                token_url=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=[f"{base_url}/.default"],
            ),
        )
    )
    data = {"ExtractionPipelineExtId": "ep_ctx_files_oid_fileshare_pandid_annotation"}

    # Locally test function handler:
    handle(data, client)


if __name__ == "__main__":
    run_locally()
