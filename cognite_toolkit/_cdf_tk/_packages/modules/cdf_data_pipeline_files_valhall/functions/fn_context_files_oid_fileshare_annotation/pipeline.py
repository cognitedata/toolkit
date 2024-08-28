from __future__ import annotations

import re
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Annotation,
    AnnotationFilter,
    Asset,
    ExtractionPipelineRun,
    FileMetadata,
    FileMetadataList,
    FileMetadataUpdate,
)
from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._text import shorten

sys.path.append(str(Path(__file__).parent))

from config import AnnotationConfig
from constants import (
    ANNOTATION_RESOURCE_TYPE,
    ANNOTATION_STATUS_APPROVED,
    ANNOTATION_STATUS_SUGGESTED,
    ASSET_ANNOTATION_TYPE,
    ASSET_MAX_LEN_META,
    CREATING_APP,
    CREATING_APPVERSION,
    FILE_ANNOTATED_META_KEY,
    FILE_ANNOTATION_TYPE,
    ISO_8601,
    ORG_MIME_TYPE,
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


def annotate_pnid(client: CogniteClient, config: AnnotationConfig) -> None:
    """
    Read configuration and start P&ID annotation process by
    1. Reading files to annotate
    2. Get file entities to be matched against files in P&ID
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
            all_files, files_to_process = get_files(client, asset_root_xid, config)
            error_count, annotated_count = 0, 0

            # if no files to annotate - continue to next asset
            if len(files_to_process) > 0:
                entities = get_files_entities(all_files)
                annotation_list = get_existing_annotations(client, entities) if entities else {}

                if files_to_process:
                    append_asset_entities(entities, client, asset_root_xid)
                    annotated_count, error_count = process_files(
                        client, entities, files_to_process, annotation_list, config
                    )
            msg = (
                f"Annotated P&ID files for asset: {asset_root_xid} number of files annotated: {annotated_count}, "
                f"file not annotated due to errors: {error_count}"
            )
            print(f"[INFO] {msg}")
            update_extpipe_run(client, config.extpipe_xid, "success", msg)

        except Exception as e:
            msg = (
                f"Annotated P&ID files failed on root asset: {asset_root_xid}. "
                f"Message: {e!s}, traceback:\n{traceback.format_exc()}"
            )
            print(f"[ERROR] {msg}")
            update_extpipe_run(client, config.extpipe_xid, "failure", shorten(msg, 1000))


def update_extpipe_run(client, xid, status, message):
    client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(extpipe_external_id=xid, status=status, message=message)
    )


def get_file_list(client: CogniteClient, asset_root_xid: str, config: AnnotationConfig) -> FileMetadataList:
    """
    Get list of files based on doc_type and mime_type to find P&ID files
    """
    return client.files.list(
        metadata={config.doc_type_meta_col: config.pnid_doc_type},
        data_set_external_ids=[config.doc_data_set_xid],
        asset_subtree_external_ids=[asset_root_xid],
        mime_type=ORG_MIME_TYPE,
        limit=config.doc_limit,
        uploaded=True,
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
    all_pnid_files: dict[str, FileMetadata] = {}
    pnids_to_process: dict[str, FileMetadata] = {}
    meta_file_update: list[FileMetadataUpdate] = []
    print(
        f"[INFO] Get files to annotate data set: {config.doc_data_set_xid!r}, asset root: {asset_root_xid!r}, "
        f"doc_type: {config.pnid_doc_type!r} and mime_type: {ORG_MIME_TYPE!r}"
    )
    file_list = get_file_list(client, asset_root_xid, config)
    for file in file_list:
        all_pnid_files[file.external_id] = file

        if file.external_id is not None and FILE_ANNOTATED_META_KEY not in (file.metadata or {}):
            pnids_to_process[file.external_id] = file

        # if run all - remove metadata element from last annotation
        elif config.run_all:
            if not config.debug:
                meta_file_update.append(
                    FileMetadataUpdate(external_id=file.external_id).metadata.remove([FILE_ANNOTATED_META_KEY])
                )
            if file.external_id is not None:
                pnids_to_process[file.external_id] = file
        else:
            update_file_metadata(meta_file_update, file, pnids_to_process)
        if config.debug:
            break

    client.files.update(meta_file_update)
    return all_pnid_files, pnids_to_process


def get_files_entities(pnid_files: dict[str, FileMetadata]) -> list[Entity]:
    """
    Loop found P&ID files and create a list of entities used for matching against file names in P&ID

    Args:
        pnid_files: Dict of files found based on filter
    """
    entities: list[Entity] = []

    for file_xid, file_meta in pnid_files.items():
        fname_list = []
        if file_meta.name is None:
            print(f"[WARNING] No name found for file with external ID: {file_xid}, and metadata: {file_meta}")
            continue

        # build list with possible file name variations used in P&ID to refer to other P&ID
        split_name = re.split("[,._ \\-!?:]+", file_meta.name)

        core_name, next_name = "", ""
        for name in reversed(split_name):
            if core_name == "":
                idx = file_meta.name.find(name)
                core_name = file_meta.name[: idx - 1]
                fname_list.append(core_name)
                continue

            if idx := core_name.find(name + next_name):
                ctx_name = core_name[idx:]
                if next_name != "":  # Ignore first part of name in matching
                    fname_list.append(ctx_name)
                next_name = core_name[idx - 1 :]

        # add entities for files used to match between file references in P&ID to other files
        entities.append(
            Entity(external_id=file_xid, org_name=file_meta.name, name=fname_list, id=file_meta.id, type="file")
        )
    return entities


def update_file_metadata(
    meta_file_update: list[FileMetadataUpdate],
    file: FileMetadata,
    pnid_files: dict[str, FileMetadata],
) -> None:
    # Parse date from metadata:
    annotated_date, annotated_stamp = None, None
    if timestamp := (file.metadata or {}).get(FILE_ANNOTATED_META_KEY):
        annotated_date = datetime.strptime(timestamp, ISO_8601)
        annotated_stamp = int(annotated_date.timestamp() * 1000)

    # live 1 h for buffer
    if annotated_stamp and file.uploaded_time and file.uploaded_time > annotated_stamp:
        meta_file_update.append(
            FileMetadataUpdate(external_id=file.external_id).metadata.remove([FILE_ANNOTATED_META_KEY])
        )
        if file.external_id is not None:
            pnid_files[file.external_id] = file


def get_existing_annotations(client: CogniteClient, entities: list[Entity]) -> dict[Optional[int], list[Optional[int]]]:
    """
    Read list of already annotated files and get corresponding annotations

    :param client: Dict of files found based on filter
    :param entities:

    :returns: dictionary of annotations
    """
    annotated_file_text: dict[Optional[int], list[Optional[int]]] = defaultdict(list)

    print("[INFO] Get existing annotations based on annotated_resource_type= file, and filtered by found files")
    file_ids = [{"id": item.id} for item in entities]

    for sub_file_list in split_into_chunks(file_ids, 1000):
        annotation_list = client.annotations.list(
            AnnotationFilter(annotated_resource_type="file", annotated_resource_ids=sub_file_list),
            limit=None,
        )
        for annotation in annotation_list:
            # only get old annotations created by this app - do not touch manual or other created annotations
            if annotation.creating_app == CREATING_APP:
                annotated_file_text[annotation.annotated_resource_id].append(annotation.id)
    return annotated_file_text


def append_asset_entities(entities: list[Entity], client: CogniteClient, asset_root_xid: str) -> None:
    """Get Asset used as input to contextualization and append to 'entities' list

    Args:
        client: Instance of CogniteClient
        asset_root_xid: external root asset ID
        entities: list of entites found so fare (file names)
    """
    print(f"[INFO] Get assets based on asset_subtree_external_ids = {asset_root_xid}")
    assets = client.assets.list(asset_subtree_external_ids=[asset_root_xid], limit=-1)

    for asset in assets:
        name = asset.name
        try:
            # clean up dummy tags and system numbers (ignore system asset names (01, 02, ...))
            if name is None or len(name) < 3 or tag_is_dummy(asset):
                continue

            # Split name - and if a system number is used also add name without system number to list
            split_name = re.split("[,._ \\-:]+", name)
            # ignore system asset if there are less than 3 tokens, ex Blind falnge, Skipp Tray, etc
            if len(split_name) < 3:
                continue

            # Add core name to list of synonyms used for name detection in P&ID
            names = [name]

            if split_name[0].isnumeric():
                names.append(name[len(split_name[0]) + 1 :])

            if split_name[0] + ":" in name:
                names.append(name[len(split_name[0]) + 1 :])

            # add wildcards as second element to tag
            names.append(f"{split_name[0]}-xxx-{name[len(split_name[0])+1:]}")

            # if name ends with 4 or more digits - add a new name with x instead of last third digit as a wild card
            if split_name[-1].isnumeric() and len(split_name[-1]) > 3:
                names.append(name[0 : (len(name) - 3)] + "x" + name[(len(name) - 2) :])

            entities.append(Entity(external_id=asset.external_id, org_name=name, name=names, id=asset.id, type="asset"))

        except Exception as e:
            print(
                f"[ERROR] Not able to get entities for asset name: {name}, id {asset.external_id}. "
                f"Error: {type(e)}({e})"
            )


def tag_is_dummy(asset: Asset) -> bool:
    custom_description = (asset.metadata or {}).get("Description", "")
    return "DUMMY TAG" in custom_description.upper()


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
        try:
            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                client, config.match_threshold, file_xid, entities, annotation_list
            )
            # create a string of matched tag - to be added to metadata
            asset_names = shorten(",".join(map(str, entities_name_found)), ASSET_MAX_LEN_META)

            # merge existing assets with new-found, and create a list without duplicates
            file_asset_ids = file.asset_ids or []
            asset_ids_list = list(set(file_asset_ids + entities_id_found))

            # If list of assets more than 1000 items, cut the list at 1000
            if len(asset_ids_list) > 1000:
                print(
                    f"[WARNING] List of assetsIds for file {file.external_id} > 1000 ({len(asset_ids_list)}), "
                    "cutting list at 1000 items"
                )
                asset_ids_list = asset_ids_list[:1000]

            if config.debug:
                print(f"[INFO] Assets found: {asset_names}")
                continue

            annotated_count += 1
            # using local time, since file upload time also uses local time
            timestamp = datetime.now().strftime(ISO_8601)
            my_update = (
                FileMetadataUpdate(id=file.id)
                .asset_ids.set(asset_ids_list)
                .metadata.add({FILE_ANNOTATED_META_KEY: timestamp, "tags": asset_names})
            )
            safe_files_update(client, my_update, file.external_id)

        except Exception as e:
            error_count += 1
            print(f"[ERROR] Failed to annotate the document: {file_xid!r}, error: {type(e)}({e})")

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
    print(f"[INFO] Detect annotations for file : {file_xid}")
    job = retrieve_diagram_with_retry(client, entities, file_xid)
    if "items" not in job.result or not job.result["items"]:
        return [], []

    if job.error_message:
        raise Exception(f"Error in contextualization job: {job.error_message}")

    detected_count = 0
    entities_id_found = []
    entities_name_found = []
    create_annotation_list: list[Annotation] = []
    to_delete_annotation_list: list[int] = []

    # build a list of annotation BEFORE filtering on matchThreshold
    annotated_resource_id = job.result["items"][0]["fileId"]
    if annotated_resource_id in annotation_list:
        to_delete_annotation_list.extend(annotation_list[annotated_resource_id])

    detected_system_num, detected_count = get_sys_nums(job.result["items"][0]["annotations"], detected_count)
    for item in job.result["items"][0]["annotations"]:
        textRegion = get_coordinates(item["region"]["vertices"])

        for entity in item["entities"]:
            if entity["type"] == "file":
                annotation_type, ref_type, txt_value = FILE_ANNOTATION_TYPE, "fileRef", entity["orgName"]
            else:
                annotation_type, ref_type, txt_value = ASSET_ANNOTATION_TYPE, "assetRef", entity["orgName"]

            # default status is suggested
            annotation_status = ANNOTATION_STATUS_SUGGESTED
            # logic to create suggestions for annotations if system number is missing from tag in P&ID
            # but a suggestion matches the most frequent system number from P&ID
            tokens = item["text"].split("-")
            if len(tokens) == 2 and item["confidence"] >= match_threshold and len(item["entities"]) == 1:
                sys_token_found = txt_value.split("-")
                if len(sys_token_found) == 3:
                    sys_num_found = sys_token_found[0]
                    # if missing system number is in > 30% of the tag assume that it's correct -
                    # else create a suggestion
                    if (
                        sys_num_found in detected_system_num
                        and detected_system_num[sys_num_found] / detected_count > 0.3
                    ):
                        annotation_status = ANNOTATION_STATUS_APPROVED
                    else:
                        annotation_status = ANNOTATION_STATUS_SUGGESTED
                else:
                    continue

            elif item["confidence"] >= match_threshold:
                annotation_status = ANNOTATION_STATUS_APPROVED

            # If there are long asset names a lower confidence is ok to create a suggestion
            elif item["confidence"] >= 0.5 and entity["type"] == "asset" and len(tokens) > 5:
                annotation_status = ANNOTATION_STATUS_SUGGESTED
            else:
                continue

            if annotation_status == ANNOTATION_STATUS_APPROVED and annotation_type == ASSET_ANNOTATION_TYPE:
                entities_name_found.append(txt_value)
                entities_id_found.append(entity["id"])

            create_annotation_list.append(
                Annotation(
                    annotation_type=annotation_type,
                    data={
                        ref_type: {"id": entity["id"]},
                        "pageNumber": item["region"]["page"],
                        "text": txt_value,
                        "textRegion": textRegion,
                    },
                    status=annotation_status,
                    annotated_resource_type=ANNOTATION_RESOURCE_TYPE,
                    annotated_resource_id=annotated_resource_id,
                    creating_app=CREATING_APP,
                    creating_app_version=CREATING_APPVERSION,
                    creating_user=f"job.{job.job_id}",
                )
            )

            # Create annotations once we hit 1k (to spread insertion over time):
            if len(create_annotation_list) == 1000:
                client.annotations.create(create_annotation_list)
                create_annotation_list.clear()

    client.annotations.create(create_annotation_list)
    safe_delete_annotations(to_delete_annotation_list, client)
    # De-duplicate list of names and id before returning:
    return list(set(entities_name_found)), list(set(entities_id_found))


def retrieve_diagram_with_retry(
    client: CogniteClient, entities: list[Entity], file_id: str, retries: int = 3
) -> DiagramDetectResults:
    for retry_num in range(1, retries + 1):
        try:
            return client.diagrams.detect(
                file_external_ids=[file_id],
                search_field="name",
                entities=[e.dump() for e in entities],
                partial_match=True,
                min_tokens=2,
            )
        except Exception as e:
            # retry func if CDF api returns an error
            if retry_num < 3:
                print(f"[WARNING] Failed to detect entities, retry #{retry_num}, error: {type(e)}({e})")
                time.sleep(retry_num * 5)
            else:
                msg = f"Failed to detect entities, error: {type(e)}({e})"
                print(f"[ERROR] {msg}")
                raise RuntimeError(msg)


def get_sys_nums(annotations: Any, detected_count: int) -> tuple[dict[str, int], int]:
    """Get dict of used system number in P&ID. The dict is used to annotate if system
    number is missing - but then only annotation of found text is part of most
    frequent used system number

    Args:
        annotations: list of annotations found by context api
        detected_count: total number of detected system numbers

    Returns:
        tuple[dict[str, int], int]: dict of system numbers and number of times used
    """
    detected_system_num = defaultdict(int)
    for item in annotations:
        tokens = item["text"].split("-")
        if len(tokens) == 3:
            detected_count += 1
            detected_system_num[tokens[0]] += 1

    return dict(detected_system_num), detected_count


def get_coordinates(vertices: list[dict]) -> dict[str, int]:
    """Get coordinates for text box based on input from contextualization
    and convert it to coordinates used in annotations.

    Args:
        vertices (list[dict]): coordinates from contextualization

    Returns:
        dict[str, int]: coordinates used by annotations.
    """
    x_min, *_, x_max = sorted(min(1, vert["x"]) for vert in vertices)
    y_min, *_, y_max = sorted(min(1, vert["y"]) for vert in vertices)

    # Adjust if min and max are equal
    if x_min == x_max:
        x_min, x_max = (x_min - 0.001, x_max) if x_min > 0.001 else (x_min, x_max + 0.001)
    if y_min == y_max:
        y_min, y_max = (y_min - 0.001, y_max) if y_min > 0.001 else (y_min, y_max + 0.001)

    return {"xMax": x_max, "xMin": x_min, "yMax": y_max, "yMin": y_min}


def safe_delete_annotations(delete_annotation_list: list[int], client: CogniteClient) -> None:
    """
    Clean up / delete exising annotations

    Handles any exception and log error if delete fails

    Args:
        delete_annotation_list: list of annotation IDs to be deleted
        client: CogniteClient
    """
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
