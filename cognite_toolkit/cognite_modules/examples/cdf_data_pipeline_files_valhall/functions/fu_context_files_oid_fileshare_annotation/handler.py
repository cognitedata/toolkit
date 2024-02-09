import logging
import os
import re
import time
import traceback

from datetime import datetime, timedelta
from time import gmtime, strftime
from typing import Any, Optional

import yaml

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Annotation,
    AnnotationFilter,
    ExtractionPipelineRun,
    FileMetadata,
    FileMetadataUpdate,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.logger import configure_logger


# P&ID original file defaults
orgMimeType = "application/pdf"
file_annotated = "file_annotated"
annotationErrorMsg = "annotation_created_error"

# Annotation defaults
assetAnnotationType = "diagrams.AssetLink"
fileAnnotationType = "diagrams.FileLink"
annotationStatusAproved = "approved"
annotationStatusSuggested = "suggested"
annotatedResourceType = "file"
creatingApp = "P&ID contextualization and annotation function"
creatingAppVersion = "1.0.0"

# Asset constats
maxLengthMetadata = 10000

# static variables
functionName = "P&ID Annotation"


# logging the output
# Configure application logger (only done ONCE):
configure_logger(logger_name="func", log_json=False, log_level="INFO")

# The following line must be added to all python modules (after imports):
logger = logging.getLogger(f"func.{__name__}")
logger.info("---------------------------------------START--------------------------------------------")


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    msg = ""
    logger.info("[STARTING] Extracting input data")

    try:
        annoConfig = getConfigParam(client, data)
        logger.info("[FINISHED] Extracting input parameters")
        annotate_p_and_id(client, annoConfig)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {functionName}: Extraction failed - Message: {e!r} - {tb}"
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


def getConfigParam(cognite_client: CogniteClient, data: dict[str, Any]) -> dict[str, Any]:
    annoConfig: dict[str, Any] = {}

    extractionPipelineExtId = data["ExtractionPipelineExtId"]

    try:
        pipeline_config_str = cognite_client.extraction_pipelines.config.retrieve(extractionPipelineExtId)
        if pipeline_config_str and pipeline_config_str != "":
            data = yaml.safe_load(pipeline_config_str.config)["data"]
        else:
            raise Exception("No configuration found in pipeline")
    except Exception as e:
        raise Exception(f"Not able to load pipeline : {extractionPipelineExtId} configuration - {e}")

    annoConfig["ExtractionPipelineExtId"] = extractionPipelineExtId
    annoConfig["debug"] = data["debug"]
    annoConfig["runAll"] = data["runAll"]
    annoConfig["docLimit"] = data["docLimit"]
    annoConfig["docDataSetExtId"] = data["docDataSetExtId"]
    annoConfig["docTypeMetaCol"] = data["docTypeMetaCol"]
    annoConfig["pAndIdDocType"] = data["pAndIdDocType"]
    annoConfig["assetRootExtIds"] = data["assetRootExtIds"]
    annoConfig["matchTreshold"] = data["matchTreshold"]

    return annoConfig


def annotate_p_and_id(cognite_client: CogniteClient, annoConfig: dict[str, Any]) -> None:
    """
    Read configuration and start P&ID annotation process by
    1. Reading files to annotate
    2. Get file entites to be matched aganst files in P&ID
    3. Read existing annotations for the found files
    4. Get assets and put it into the list of entites to be found in the P&ID
    5. Process file:
        - detecting entities
        - creation annotations.
        - remove duplicate annotations

    :param cognite_client: Dict of files found based on filter
    :param config:
    """

    logger.info("Initiating Annotation of P&ID")

    extractionPipelineExtId = annoConfig["ExtractionPipelineExtId"]
    debug = annoConfig["debug"]
    runAll = annoConfig["runAll"]
    docLimit = annoConfig["docLimit"]
    docDataSetExtId = annoConfig["docDataSetExtId"]
    docTypeMetaCol = annoConfig["docTypeMetaCol"]
    pAndIdDocType = annoConfig["pAndIdDocType"]
    assetRootExtIds = annoConfig["assetRootExtIds"]
    matchTreshold = annoConfig["matchTreshold"]

    for assetRootExtId in assetRootExtIds:
        try:
            files_all, files_process = get_files(
                cognite_client,
                docDataSetExtId,
                assetRootExtId,
                docTypeMetaCol,
                pAndIdDocType,
                runAll,
                debug,
                docLimit,
            )
            entities = get_files_entities(files_all)

            if len(entities) > 0:
                annotationList = get_existing_annotations(cognite_client, entities)

            numAnnotated = 0
            numErrors = 0
            if len(files_process) > 0:
                entities = get_assets(cognite_client, assetRootExtId, entities)
                numAnnotated, numErrors = process_files(
                    cognite_client,
                    matchTreshold,
                    entities,
                    files_process,
                    annotationList,
                    debug,
                )

            msg = f"Annotated P&ID files for asset: {assetRootExtId} number of files annotated: {numAnnotated} - file not annotaded due to errors: {numErrors}"
            logger.info(msg)
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=extractionPipelineExtId,
                    status="success",
                    message=msg,
                )
            )

        except (CogniteAPIError, Exception) as e:
            msg = f"Annotated P&ID files failed on root asset: {assetRootExtId} failed - Message: {e!s}"
            logger.exception(msg)
            if len(msg) > 1000:
                msg = msg[0:995] + "..."
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=extractionPipelineExtId,
                    status="failure",
                    message=msg,
                )
            )
            pass


def get_files(
    cognite_client: CogniteClient,
    docDataSetExtId: str,
    assetRootExtId: str,
    docTypeMetaCol: str,
    pAndIdDocType: str,
    runAll: bool,
    debug: bool,
    docLimit: int = -1,
) -> tuple[dict[str, FileMetadata], dict[str, FileMetadata]]:
    """
    Read files based on doc_type and mime_type to find P&ID files

    :param cognite_client:
    :param pAndIdDocType:
    :param runAll:
    :param debug:
    :param docLimit:

    :returns: dict of files
    """

    pAndIdFiles_all: dict[str, FileMetadata] = {}  # Define a type for Dict
    pAndIdFiles_process: dict[str, FileMetadata] = {}  # Define a type for Dict
    numDoc = 0
    metaFileUpdate: list[FileMetadataUpdate] = []

    logger.info(
        f"Get files to annotate data set: {docDataSetExtId}, asset root: {assetRootExtId} doc_type: {pAndIdDocType} and mime_type: {orgMimeType}"
    )

    file_list = cognite_client.files.list(
        metadata={docTypeMetaCol: pAndIdDocType},
        data_set_external_ids=[docDataSetExtId],
        mime_type=orgMimeType,
        limit=docLimit,
    )

    for file in file_list:
        numDoc += 1
        pAndIdFiles_all[file.external_id] = file

        # only process files related to docLimit (-1 == ALL)
        if docLimit == -1 or numDoc <= docLimit:
            # debug
            # if file.name == "SKA-AK-ER251-R-XB-2011-001.pdf":
            #    pAndIdFiles_process[file.external_id] = file
            #    break
            # else:
            #    continue

            if file_annotated is not None and file_annotated not in (file.metadata or {}):
                if file.external_id is not None:
                    pAndIdFiles_process[file.external_id] = file

            # if run all - remove metadata element from last annotation
            elif runAll:
                if not debug and file_annotated is not None:
                    file_meta_update = FileMetadataUpdate(external_id=file.external_id).metadata.remove(
                        [file_annotated]
                    )
                    metaFileUpdate.append(file_meta_update)
                if file.external_id is not None:
                    pAndIdFiles_process[file.external_id] = file
            else:
                pAndIdFiles_process, metaFileUpdate = update_file_metadata(
                    metaFileUpdate, file, pAndIdFiles_process, file_annotated
                )
        if debug:
            break

    if len(metaFileUpdate) > 0:
        cognite_client.files.update(metaFileUpdate)

    return pAndIdFiles_all, pAndIdFiles_process


def update_file_metadata(
    metaFileUpdate: list[FileMetadataUpdate],
    file: FileMetadata,
    pAndIdFiles: dict[str, FileMetadata],
    file_annotated: str,
) -> tuple[dict[str, FileMetadata], list[FileMetadataUpdate]]:

    annotated_date = None
    if file.metadata and file_annotated is not None:
        file_annotated_time = file.metadata.get(file_annotated, None)
        if file_annotated_time:
            annotated_date = datetime.strptime(file_annotated_time, "%Y-%m-%d %H:%M:%S")

    annotated_stamp = int(annotated_date.timestamp() * 1000) if annotated_date else None
    if (
        annotated_stamp is not None and file.last_updated_time is not None and file.last_updated_time > annotated_stamp
    ):  # live 1 h for buffer
        if file_annotated is not None:  # Check for None
            file_meta_update = FileMetadataUpdate(external_id=file.external_id).metadata.remove([file_annotated])
            metaFileUpdate.append(file_meta_update)
        if file.external_id is not None:
            pAndIdFiles[file.external_id] = file

    return pAndIdFiles, metaFileUpdate


def get_files_entities(pAndIdFiles: dict[str, FileMetadata]) -> list[dict[str, Any]]:
    """
    Loop found P&ID files and create list of enties used for matching against file names in P&ID

    :param pAndIdFiles: Dict of files found based on filter

    :returns: list of entities
    """

    entities = []
    numDoc = 0

    for file_extId, file_meta in pAndIdFiles.items():
        numDoc += 1
        fnameList = []
        if file_meta.name is None:
            logger.warning(f"No name found for file with external ID: {file_extId}, and metadata: {file_meta}")
            continue

        # build list with possible file name variations used in P&ID to refer to other P&ID
        split_name = re.split("[,._ \\-!?:]+", file_meta.name)

        ctx_name = ""
        core_name = ""
        next_name = ""
        for name in reversed(split_name):
            if core_name == "":
                idx = file_meta.name.find(name)
                core_name = file_meta.name[: idx - 1]
                fnameList.append(core_name)
            else:
                idx = core_name.find(name + next_name)
                if idx != 0:
                    ctx_name = core_name[idx:]
                    if next_name != "":  # Ignore first part of name in matching
                        fnameList.append(ctx_name)
                    next_name = core_name[idx - 1 :]

        # add entities for files used to match between file references in P&ID to other files
        entities.append(
            {
                "externalId": file_extId,
                "orgName": file_meta.name,
                "name": fnameList,
                "id": file_meta.id,
                "type": "file",
            }
        )

    return entities


def get_existing_annotations(
    cognite_client: CogniteClient, entities: list[dict[str, Any]]
) -> dict[Optional[int], list[Optional[int]]]:
    """
    Read list of already annotated files and get corresponding annotations

    :param cognite_client: Dict of files found based on filter
    :param entities:

    :returns: dictionary of annotations
    """

    fileList = []
    annotationList = None
    annotatedFileText: dict[Optional[int], list[Optional[int]]] = {}

    logger.info("Get existing annotations based on annotated_resource_type= file, and filtered by found files")
    for item in entities:
        fileList.append({"id": item["id"]})

    n = 1000
    for i in range(0, len(fileList), n):
        subFileList = fileList[i : i + n]

        if len(subFileList) > 0:
            annotateFilter = AnnotationFilter(annotated_resource_type="file", annotated_resource_ids=subFileList)
            annotationList = cognite_client.annotations.list(limit=-1, filter=annotateFilter)

        for anno in annotationList or []:
            # only get old annotations created by this app - do not thouch manual or other created annotations
            if anno.creating_app == creatingApp:
                annotated_resource_id = anno.annotated_resource_id
                if annotated_resource_id not in annotatedFileText:
                    annotatedFileText[annotated_resource_id] = [anno.id]
                else:
                    annotatedFileText[annotated_resource_id].append(anno.id)

    return annotatedFileText


def get_assets(
    cognite_client: CogniteClient,
    assetRootExtId: str,
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Get Asset used as input to contextualization

    :param cognite_client: Dict of files found based on filter
    :param assetRootExtId: external root asset ID
    :param assetPreProcessing: list of asset preprocessing functions connected to root asset
    :param entities: list of entites found so fare (file names)

    :returns: list of entities
    """

    logger.info(f"Get assets based on asset_subtree_external_ids = {assetRootExtId}")
    assets = cognite_client.assets.list(asset_subtree_external_ids=[assetRootExtId], limit=-1)

    # clean up dummy tags ans system numbers
    for asset in assets:
        try:
            nameList = []
            name = asset.name
            notDymmy = True
            if (
                asset.metadata is not None
                and "Description" in asset.metadata
                and "DUMMY TAG" in asset.metadata.get("Description", "").upper()
            ):
                notDymmy = False
            if name is not None and len(name) > 3 and notDymmy:  # ignore system asset names (01, 02, ...)
                nameList.append(name)

                # Split name - and if a system number is used also add name without system number to list
                split_name = re.split("[,._ \\-:]+", name)
                if split_name[0].isnumeric():
                    nameList.append(name[len(split_name[0]) + 1 :])

                entities.append(
                    {
                        "externalId": asset.external_id,
                        "orgName": name,
                        "name": nameList,
                        "id": asset.id,
                        "type": "asset",
                    }
                )
        except Exception:
            logger.error(f"Not able to get entities for asset name: {name}, id {asset.external_id}")
            pass

    return entities


def process_files(
    cognite_client: CogniteClient,
    matchTreshold: float,
    entities: list[dict[str, Any]],
    files: dict[str, FileMetadata],
    annotationList: dict[Optional[int], list[Optional[int]]],
    debug: bool,
) -> tuple[int, int]:
    """
    Contextualize files by calling the annotation function
    Then update the metadata for the P&ID input file

    :param cognite_client: client id used to connect to CDF
    :param matchTreshold: score used to qualify match
    :param entities: list of input entities that are used to match content in file
    :param annotationList: list of existing annotations for input files
    :param debug: debug flag, if tru do not update original file

    :returns: number of annotated files and number of errors
    """
    numAnnotated = 0
    numErrors = 0
    for fileId, file in files.items():
        logger.info(f"Parse and annotate, input file: {fileId}")

        try:
            annotationList = annotationList or {}

            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                cognite_client, matchTreshold, fileId, entities, annotationList
            )

            # create a string of matched tag - to be added to metadata
            assetNames = ",".join(map(str, entities_name_found))
            if len(assetNames) > maxLengthMetadata:
                assetNames = assetNames[0:maxLengthMetadata] + "..."

            file_asset_ids = list(file.asset_ids) if file.asset_ids else []

            # merge existing assetids with new found, and create a list without duplicates
            assetIdsList = list(set(file_asset_ids + entities_id_found))

            # If list of assets more than 1000 items, cut the list at 1000
            if len(assetIdsList) > 1000:
                logger.warning(
                    f"List of assetsIds for file {file.external_id} > 1000 ({len(assetIdsList)}), cutting list at 1000 items"
                )
                assetIdsList = assetIdsList[:1000]

            if not debug:
                numAnnotated += 1
                # Update metadata from found PDF files
                try:
                    # Note uses local time, since file update time also uses local time + add a minute
                    # making sure annotation time is larger than last update time
                    now = datetime.now() + timedelta(minutes=1)
                    convert_date_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    my_update = (
                        FileMetadataUpdate(id=file.id)
                        .asset_ids.set(assetIdsList)
                        .metadata.add({file_annotated: convert_date_time, "tags": assetNames})
                    )
                    updateOrgFiles(cognite_client, my_update, file.external_id)
                except Exception as e:
                    s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
                    logger.warning(f"Not able to update refrence doc : {fileId} - {s}  - {r}")
                    pass

            else:
                logger.info(f"Converted and created (not upload due to DEBUG) file: {fileId}")
                logger.info(f"Assets found: {assetNames}")

        except Exception as e:
            numErrors += 1
            s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
            msg = f"ERROR: Failed to annotate the document, Message: {s}  - {r}"
            logger.error(msg)
            if "KeyError" in r:
                convert_date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
                my_update = FileMetadataUpdate(id=file.id).metadata.add(
                    {file_annotated: convert_date_time, annotationErrorMsg: msg}
                )
                updateOrgFiles(cognite_client, my_update, file.external_id)
            pass

    return numAnnotated, numErrors


def detect_create_annotation(
    cognite_client: CogniteClient,
    matchTreshold: float,
    fileId: str,
    entities: list,
    annotationList: dict,
) -> tuple[list[Any], list[Any]]:
    """
    Detect tags + files and create annotation for P&ID

    :param cognite_client: client id used to connect to CDF
    :param matchTreshold: score used to qualify match
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
                annotationType = fileAnnotationType
                refType = "fileRef"
                txtValue = item["entities"][0]["orgName"]
            else:
                annotationType = assetAnnotationType
                refType = "assetRef"
                txtValue = item["entities"][0]["orgName"]
                entities_name_found.append(item["entities"][0]["name"][0])
                entities_id_found.append(item["entities"][0]["id"])

            # logic to create suggestions for annotations if system number is missing from tag in P&ID
            # but suggestion matches most frequent system number from P&ID
            tokens = item["text"].split("-")
            if len(tokens) == 2 and item["confidence"] >= matchTreshold and len(item["entities"]) == 1:
                sysTokenFound = item["entities"][0]["name"][0].split("-")
                if len(sysTokenFound) == 3:
                    sysNumFound = sysTokenFound[0]
                    # if missing system number is in > 30% of the tag asume it's correct - else create suggestion
                    if sysNumFound in detectedSytemNum and detectedSytemNum[sysNumFound] / numDetected > 0.3:
                        annotationStatus = annotationStatusAproved
                    else:
                        annotationStatus = annotationStatusSuggested
                else:
                    continue

            elif item["confidence"] >= matchTreshold and len(item["entities"]) == 1:
                annotationStatus = annotationStatusAproved

            # If there are long asset names a lower confidence is ok to create a suggestion
            elif item["confidence"] >= 0.5 and item["entities"][0]["type"] == "asset" and len(tokens) > 5:
                annotationStatus = annotationStatusSuggested
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
                annotated_resource_type=annotatedResourceType,
                annotated_resource_id=annotatedResourceId,
                creating_app=creatingApp,
                creating_app_version=creatingAppVersion,
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
