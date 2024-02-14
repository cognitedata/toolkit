import logging
import os
import re
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import gmtime, strftime
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
from cognite.client.exceptions import CogniteAPIError
from cognite.logger import configure_logger

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

# Asset constats
MAX_LENGTH_METADATA = 10000

# static variables
FUNCTION_NAME = "P&ID Annotation"

# logging the output
# Configure application logger (only done ONCE):
configure_logger(logger_name="func", log_json=False, log_level="INFO")

# The following line must be added to all python modules (after imports):
logger = logging.getLogger(f"func.{__name__}")
logger.info("---------------------------------------START--------------------------------------------")


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
    def load(cls, data: dict[str, Any]) -> "AnnotationConfig":
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


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    msg = ""
    logger.info("[STARTING] Extracting input data")

    try:
        config = load_config_parameters(client, data)
        logger.info("[FINISHED] Extracting input parameters")
        annotate_p_and_id(client, config)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {FUNCTION_NAME}: Extraction failed - Message: {e!r} - {tb}"
        logger.error(f"[FAILED] Error: {msg}")
        return {
            "error": e.__str__(),
            "status": "failed",
            "data": data,
            "secrets": mask_secrets(secrets),
            "functionInfo": function_call_info,
        }

    logger.info(f"[FINISHED] : {msg}")

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

    logger.info("Initiating Annotation of P&ID")

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

            msg = f"Annotated P&ID files for asset: {asset_root_ext_id} number of files annotated: {annotated_count} - file not annotaded due to errors: {error_count}"
            logger.info(msg)
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="success",
                    message=msg,
                )
            )

        except (CogniteAPIError, Exception) as e:
            msg = f"Annotated P&ID files failed on root asset: {asset_root_ext_id} failed - Message: {e!s}"
            logger.exception(msg)
            if len(msg) > 1000:
                msg = msg[0:995] + "..."
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="failure",
                    message=msg,
                )
            )
            pass


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

    logger.info(
        f"Get files to annotate data set: {config.doc_data_set_ext_id}, asset root: {asset_root_ext_id} doc_type: "
        f"{config.p_and_id_doc_type} and mime_type: {ORG_MIME_TYPE}"
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
                annotated_date = datetime.strptime(file_annotated_time, "%Y-%m-%d %H:%M:%S")
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
    Loop found P&ID files and create list of enties used for matching against file names in P&ID

    Args:
        p_and_id_files: Dict of files found based on filter
    """
    entities: list[Entity] = []
    doc_count = 0

    for file_ext_id, file_meta in p_and_id_files.items():
        doc_count += 1
        fname_list = []
        if file_meta.name is None:
            logger.warning(f"No name found for file with external ID: {file_ext_id}, and metadata: {file_meta}")
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

    logger.info("Get existing annotations based on annotated_resource_type= file, and filtered by found files")
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

    logger.info(f"Get assets based on asset_subtree_external_ids = {asset_root_ext_id}")
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
            logger.error(f"Not able to get entities for asset name: {name}, id {asset.external_id}")
            pass


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

    for file_id, file in files.items():
        logger.info(f"Parse and annotate, input file: {file_id}")

        try:
            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                cognite_client, config.match_threshold, file_id, entities, annotation_list
            )

            # create a string of matched tag - to be added to metadata
            assetNames = ",".join(map(str, entities_name_found))
            if len(assetNames) > MAX_LENGTH_METADATA:
                assetNames = assetNames[0:MAX_LENGTH_METADATA] + "..."

            file_asset_ids = list(file.asset_ids) if file.asset_ids else []

            # merge existing assets with new-found, and create a list without duplicates
            asset_ids_list = list(set(file_asset_ids + entities_id_found))

            # If list of assets more than 1000 items, cut the list at 1000
            if len(asset_ids_list) > 1000:
                logger.warning(
                    f"List of assetsIds for file {file.external_id} > 1000 ({len(asset_ids_list)}), cutting list at 1000 items"
                )
                asset_ids_list = asset_ids_list[:1000]

            if not config.debug:
                annotated_count += 1
                # Update metadata from found PDF files
                try:
                    # Note uses local time, since file update time also uses local time + add a minute
                    # making sure annotation time is larger than last update time
                    now = datetime.now() + timedelta(minutes=1)
                    convert_date_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    my_update = (
                        FileMetadataUpdate(id=file.id)
                        .asset_ids.set(asset_ids_list)
                        .metadata.add({FILE_ANNOTATED_METADATA_KEY: convert_date_time, "tags": assetNames})
                    )
                    updateOrgFiles(cognite_client, my_update, file.external_id)
                except Exception as e:
                    s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
                    logger.warning(f"Not able to update refrence doc : {file_id} - {s}  - {r}")
                    pass

            else:
                logger.info(f"Converted and created (not upload due to DEBUG) file: {file_id}")
                logger.info(f"Assets found: {assetNames}")

        except Exception as e:
            error_count += 1
            s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
            msg = f"ERROR: Failed to annotate the document, Message: {s}  - {r}"
            logger.error(msg)
            if "KeyError" in r:
                convert_date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
                my_update = FileMetadataUpdate(id=file.id).metadata.add(
                    {FILE_ANNOTATED_METADATA_KEY: convert_date_time, ANNOTATION_ERROR_MSG: msg}
                )
                updateOrgFiles(cognite_client, my_update, file.external_id)
            pass

    return annotated_count, error_count


def detect_create_annotation(
    cognite_client: CogniteClient,
    match_threshold: float,
    fileId: str,
    entities: list,
    annotationList: dict,
) -> tuple[list[Any], list[Any]]:
    """
    Detect tags + files and create annotation for P&ID

    :param cognite_client: client id used to connect to CDF
    :param match_threshold: score used to qualify match
    :param fileId: file to be processed
    :param entities: list of input entities that are used to match content in file
    :param annotationList: list of existing annotations for input files

    :returns: list of found ID and names in P&ID
    """

    entities_id_found = []
    entities_name_found = []
    createAnnotationList: list[Annotation] = []
    deleteAnnotationList: list[int] = []
    numDetected = 0

    # in case contextualization service not is avaiable - back off and retry
    retryNum = 0
    while retryNum < 3:
        try:
            job = cognite_client.diagrams.detect(
                file_external_ids=[fileId],
                search_field="name",
                entities=entities,
                partial_match=True,
                min_tokens=2,
            )
            break
        except Exception as e:
            s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
            # retry func if CDF api returns an error
            if retryNum < 3:
                retryNum += 1
                logger.warning(f"Retry #{retryNum} - wait before retry - error was: {s}  - {r}")
                time.sleep(retryNum * 5)
                pass
            else:
                msg = f"ERROR: Failed to detect entities, Message: {s}  - {r}"
                logger.error(msg)
                raise Exception(msg)

    if "items" in job.result and len(job.result["items"]) > 0:
        # build list of annotation BEFORE filtering on matchTreshold
        annotatedResourceId = job.result["items"][0]["fileId"]
        if annotatedResourceId in annotationList:
            deleteAnnotationList.extend(annotationList[annotatedResourceId])

        detectedSytemNum, numDetected = getSysNums(job.result["items"][0]["annotations"], numDetected)
        for item in job.result["items"][0]["annotations"]:
            if item["entities"][0]["type"] == "file":
                annotationType = FILE_ANNOTATION_TYPE
                refType = "fileRef"
                txtValue = item["entities"][0]["orgName"]
            else:
                annotationType = ASSET_ANNOTATION_TYPE
                refType = "assetRef"
                txtValue = item["entities"][0]["orgName"]
                entities_name_found.append(item["entities"][0]["name"][0])
                entities_id_found.append(item["entities"][0]["id"])

            # logic to create suggestions for annotations if system number is missing from tag in P&ID
            # but suggestion matches most frequent system number from P&ID
            tokens = item["text"].split("-")
            if len(tokens) == 2 and item["confidence"] >= match_threshold and len(item["entities"]) == 1:
                sysTokenFound = item["entities"][0]["name"][0].split("-")
                if len(sysTokenFound) == 3:
                    sysNumFound = sysTokenFound[0]
                    # if missing system number is in > 30% of the tag asume it's correct - else create suggestion
                    if sysNumFound in detectedSytemNum and detectedSytemNum[sysNumFound] / numDetected > 0.3:
                        annotationStatus = ANNOTATION_STATUS_APPROVED
                    else:
                        annotationStatus = ANNOTATION_STATUS_SUGGESTED
                else:
                    continue

            elif item["confidence"] >= match_threshold and len(item["entities"]) == 1:
                annotationStatus = ANNOTATION_STATUS_APPROVED

            # If there are long asset names a lower confidence is ok to create a suggestion
            elif item["confidence"] >= 0.5 and item["entities"][0]["type"] == "asset" and len(tokens) > 5:
                annotationStatus = ANNOTATION_STATUS_SUGGESTED
            else:
                continue

            xMin, xMax, yMin, yMax = getCord(item["region"]["vertices"])

            annotationData = {
                refType: {"externalId": item["entities"][0]["externalId"]},
                "pageNumber": item["region"]["page"],
                "text": txtValue,
                "textRegion": {
                    "xMax": xMax,
                    "xMin": xMin,
                    "yMax": yMax,
                    "yMin": yMin,
                },
            }

            fileAnnotation = Annotation(
                annotation_type=annotationType,
                data=annotationData,
                status=annotationStatus,
                annotated_resource_type=ANNOTATION_RESOURCE_TYPE,
                annotated_resource_id=annotatedResourceId,
                creating_app=CREATING_APP,
                creating_app_version=CREATING_APPVERSION,
                creating_user=f"job.{job.job_id}",
            )

            createAnnotationList.append(fileAnnotation)

            # can only create 1000 annotations at the time.
            if len(createAnnotationList) >= 999:
                cognite_client.annotations.create(createAnnotationList)
                createAnnotationList = []

        if len(createAnnotationList) > 0:
            cognite_client.annotations.create(createAnnotationList)

        deleteAnnotations(deleteAnnotationList, cognite_client)

        # sort / deduplicate list of names and id
        entities_name_found = list(dict.fromkeys(entities_name_found))
        entities_id_found = list(dict.fromkeys(entities_id_found))

    return entities_name_found, entities_id_found


def getSysNums(annotations: Any, numDetected: int) -> dict[str, int]:
    """
    Get dict of used system number in P&ID. The dict is used to annotate if system
    number is missing - but then only annotation of found text is part of most
    frequent used system number

    :annotations found by context api
    :numDetected total number of detected system numbers

    :returns: dict of system numbers and number of times used.
    """

    detectedSytemNum = {}

    for item in annotations:
        tokens = item["text"].split("-")
        if len(tokens) == 3:
            sysNum = tokens[0]
            numDetected += 1
            if sysNum in detectedSytemNum:
                detectedSytemNum[sysNum] += 1
            else:
                detectedSytemNum[sysNum] = 1

    return detectedSytemNum, numDetected


def getCord(vertices: dict) -> tuple[int, int, int, int]:
    """
    Get coordinates for text box based on input from contextualization
    and convert it to coordinates used in annotations.

    :param vertices coordinates from contextualization

    :returns: coordinates used by annotations.
    """

    initValues = True

    for vert in vertices:
        # Values must be between 0 and 1
        x = 1 if vert["x"] > 1 else vert["x"]
        y = 1 if vert["y"] > 1 else vert["y"]

        if initValues:
            xMax = x
            xMin = x
            yMax = y
            yMin = y
            initValues = False
        else:
            if x > xMax:
                xMax = x
            elif x < xMin:
                xMin = x
            if y > yMax:
                yMax = y
            elif y < yMin:
                yMin = y

        if xMin == xMax:
            if xMin > 0.001:
                xMin -= 0.001
            else:
                xMax += 0.001

        if yMin == yMax:
            if yMin > 0.001:
                yMin -= 0.001
            else:
                yMax += 0.001

    return xMin, xMax, yMin, yMax


def deleteAnnotations(deleteAnnotationList: list, cognite_client: CogniteClient) -> None:
    """
    Clean up / delete exising annotatoions

    :param deleteAnnotationList: list of annotation IDs to be deleted
    :param cognite_client: Dict of files found based on filter

    :returns: None
    """

    try:
        if len(deleteAnnotationList) > 0:
            deleteL = list(set(deleteAnnotationList))
            cognite_client.annotations.delete(deleteL)
    except Exception as e:
        s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
        msg = f"Failed to delete annotations, Message: {s}  - {r}"
        logger.warning(msg)
        pass


def updateOrgFiles(
    cognite_client: CogniteClient,
    my_updates: FileMetadataUpdate,
    fileExtId: str,
) -> None:
    """
    Update metadata of original pdf files wit list of tags

    :param cognite_client: Dict of files found based on filter
    :param my_updates:
    """

    try:
        # write updates for existing files
        cognite_client.files.update(my_updates)

    except Exception as e:
        s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
        logger.error(f"Failed to update the file {fileExtId}, Message: {s}  - {r}")
        pass


def main():
    """
    Code used for local Test & Debug
    update local .env file to set variables to connect to CDF
    """

    cdfProjectName = os.environ["CDF_PROJECT"]
    cdfCluster = os.environ["CDF_CLUSTER"]
    clientId = os.environ["IDP_CLIENT_ID"]
    clientSecret = os.environ["IDP_CLIENT_SECRET"]
    tokenUri = os.environ["IDP_TOKEN_URL"]

    baseUrl = f"https://{cdfCluster}.cognitedata.com"
    scopes = f"{baseUrl}/.default"
    secrets = {"mySecrets": "Values"}
    function_call_info = {"Debugging": "Called from Function main "}

    oauth_provider = OAuthClientCredentials(
        token_url=tokenUri,
        client_id=clientId,
        client_secret=clientSecret,
        scopes=scopes,
    )

    cnf = ClientConfig(
        client_name=cdfProjectName,
        base_url=baseUrl,
        project=cdfProjectName,
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
