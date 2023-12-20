import logging
import time
from fractions import Fraction
from threading import Event
from time import gmtime, strftime
from typing import Any, Dict, List, Optional, Tuple
import traceback

import arrow
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Annotation,
    AnnotationFilter,
    ExtractionPipelineRun,
    FileMetadata,
    FileMetadataUpdate,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.statestore import AbstractStateStore

from .config import Config



# The following line must be added to all python modules (after imports):
logger = logging.getLogger(f"func.{__name__}")
logger.info("---------------------------------------START--------------------------------------------")


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
functionName = "Contextualization: P&ID Annotation"



def handle(client, data):
    msg = ""
    logger.info(f"[STARTING] {functionName}")
    logger.info("[STARTING] Extracting input data")
    try:
        annoConfig = getConfigParam(client, data)
        logger.info("[FINISHED] Extracting input parameters")
    except Exception as e:
        logger.error(f"[FAILED] Extracting input parameters. Error: {e}")
        raise e

    try:
        annotate_p_and_id(client, annoConfig)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {functionName}: Extraction failed - Message: {repr(e)} - {tb}"
        logger.error(f"[FAILED] {functionName}. Error: {msg}")
        return {"error": e.__str__(), "status": "failed"}

    logger.info(f"[FINISHED] {functionName} : {msg}")

    return {"status": "succeeded"}


    
    
def getConfigParam(
    cognite_client: CogniteClient, data: Dict[str, Any]
) -> Dict[str, Any]:
    
    annoConfig: Dict[str, Any] = {}

    extractionPipelineExtId = data["ExtractionPipelineExtId"]

    try:
        pipeline_config_str = cognite_client.extraction_pipelines.config.retrieve(extractionPipelineExtId)
        if pipeline_config_str and pipeline_config_str != "":
            data = yaml.safe_load(pipeline_config_str.config)["data"]
        else:
            raise Exception("No configuration found in pipeline")
    except Exception as e:
        logger.error(f"Not able to load pipeline : {extractionPipelineExtId} configuration - {e}")


    annoConfig["ExtractionPipelineExtId"] = extractionPipelineExtId
    annoConfig["debug"] = data["debug"]
    annoConfig["runAll"] = data["runAll"]
    annoConfig["docLimit"] = data["docLimit"]
    annoConfig["docDataSetExtId"] = data["docDataSetExtId"]
    annoConfig["docTypeMetaCol"] = data["docTypeMetaCol"]
    annoConfig["pAndIdDocType"] = data["pAndIdDocType"]
    annoConfig["assetRootExtIds"] = data["assetRootExtIds"]
    annoConfig["shortNamePrefix"] = data["shortNamePrefix"]
    annoConfig["matchTreshold"] = data["matchTreshold"]

    return annoConfig



def annotate_p_and_id(
    cognite_client: CogniteClient, annoConfig: Dict[str, Any]
) -> None:
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
    shortNamePrefix = annoConfig["shortNamePrefix"]
    matchTreshold = annoConfig["matchTreshold"]

    for assetRootExtId in assetRootExtIds:

        try:

            files_all, files_process = get_files(
                logger,
                cognite_client,
                docDataSetExtId,
                assetRootExtId,
                docTypeMetaCol,
                pAndIdDocType,
                runAll,
                debug,
                docLimit,
            )
            entities = get_files_entities(logger, files_all, shortNamePrefix, assetRootExtId)

            if len(entities) > 0:
                annotationList = get_existing_annotations(
                    logger, cognite_client, entities
                )

            if len(files_process) > 0:
                entities = get_assets(
                    logger, cognite_client, assetRootExtId, entities
                )
                numAnnotated, numErrors = process_files(
                    logger,
                    cognite_client,
                    matchTreshold,
                    entities,
                    files_process,
                    annotationList,
                    debug,
                )

                msg = f"Annotated P&ID files for asset: {assetRootExtId} was: {numAnnotated} - file errors: {numErrors}"
                logger.info(msg)
                cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=extractionPipelineExtId,
                        status="success",
                        message=msg,
                    )
                )

        except (CogniteAPIError, Exception) as e:

            msg = f"Annotated P&ID files failed on root asset: {assetRootExtId} failed - Message: {str(e)}"
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
    logger: Any,
    cognite_client: CogniteClient,
    docDataSetExtId: str,
    assetRootExtId: str,
    docTypeMetaCol: str,
    pAndIdDocType: str,
    runAll: bool,
    debug: bool,
    docLimit: int = -1,
) -> tuple[Dict[str, FileMetadata], Dict[str, FileMetadata]]:
    """
    Read files based on doc_type and mime_type to find P&ID files

    :param cognite_client:
    :param pAndIdDocType:
    :param runAll:
    :param debug:
    :param docLimit:

    :returns: dict of files
    """

    pAndIdFiles_all: Dict[str, FileMetadata] = {}  # Define a type for Dict
    pAndIdFiles_process: Dict[str, FileMetadata] = {}  # Define a type for Dict
    numDoc = 0
    metaFileUpdate: List[FileMetadataUpdate] = []

    logger.info(
        f"Get files to annotate data set: {docDataSetExtId}, asset root: {assetRootExtId} doc_type: {pAndIdDocType} and mime_type: {orgMimeType}"
    )

    file_list = cognite_client.files.list(
        metadata={docTypeMetaCol: pAndIdDocType},
        data_set_external_ids=[docDataSetExtId],
        asset_subtree_external_ids=[assetRootExtId],
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
            
            if file_annotated is not None and file_annotated not in (
                file.metadata or {}
            ):
                if file.external_id is not None:
                    pAndIdFiles_process[file.external_id] = file

            # if run all - remove metadata element from last annotation
            elif runAll:
                if not debug and file_annotated is not None:
                    file_meta_update = FileMetadataUpdate(
                        external_id=file.external_id
                    ).metadata.remove([file_annotated])
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
    metaFileUpdate: List[FileMetadataUpdate],
    file: FileMetadata,
    pAndIdFiles: Dict[str, FileMetadata],
    file_annotated: str,
) -> tuple[Dict[str, FileMetadata], List[FileMetadataUpdate]]:

    annotated_date = (
        arrow.get(file.metadata.get(file_annotated, "")).datetime
        if file.metadata and file_annotated is not None
        else None
    )
    annotated_stamp = int(annotated_date.timestamp() * 1000) if annotated_date else None
    if (
        annotated_stamp is not None
        and file.last_updated_time is not None  # Check for None
        and file.last_updated_time > annotated_stamp + 3600000
    ):  # live 1 h for buffer
        if file_annotated is not None:  # Check for None
            file_meta_update = FileMetadataUpdate(
                external_id=file.external_id
            ).metadata.remove([file_annotated])
            metaFileUpdate.append(file_meta_update)
        if file.external_id is not None:
            pAndIdFiles[file.external_id] = file

    return pAndIdFiles, metaFileUpdate


def get_files_entities(
    logger: Any,
    pAndIdFiles: Dict[str, FileMetadata],
    shortNamePrefix: Optional[Dict],
    assetRootExtId: str,
) -> List[Dict[str, Any]]:
    """
    Loop found P&ID files and create list of enties used for matching against file names in P&ID

    :param pAndIdFiles: Dict of files found based on filter
    :param shortNamePrefix:

    :returns: list of entities
    """

    entities = []
    numDoc = 0

    for file_extId, file_meta in pAndIdFiles.items():
        numDoc += 1
        fnameList = []
        if file_meta.name is not None:
            fname = file_meta.name.rsplit(".", 1)[0]
            fnameCore = fname.split(".", 1)[0]
        else:
            logger.warning(f"No name found for file with external ID: {file_extId}, and metadata: {file_meta}")
            continue

        fnameList.append(fnameCore)

        # This is used since file name references in P&ID not uses the full file name       
        if (
            shortNamePrefix
            and assetRootExtId in shortNamePrefix
            and fnameCore.find(shortNamePrefix[assetRootExtId]) > -1
        ):
            shortName = fnameCore[fnameCore.find(shortNamePrefix[assetRootExtId])
                + 1 : ]  
            fnameList.append(shortName)

            shortName = fnameCore[fnameCore.find(shortNamePrefix[assetRootExtId])
                + len(shortNamePrefix[assetRootExtId]) : ]  
            fnameList.append(shortName)
            
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
    logger: Any, cognite_client: CogniteClient, entities: List[Dict[str, Any]]
) -> Dict[Optional[int], List[Optional[int]]]:
    """
    Read list of already annotated files and get corresponding annotations

    :param cognite_client: Dict of files found based on filter
    :param entities:

    :returns: dictionary of annotations
    """

    fileList = []
    annotationList = None
    annotatedFileText: Dict[Optional[int], List[Optional[int]]] = {}

    logger.info(
        "Get existing annotations based on annotated_resource_type= file, and filtered by found files"
    )
    for item in entities:
        fileList.append({"id": item["id"]})

    n = 1000
    for i in range(0, len(fileList), n):
        subFileList =  fileList[i:i + n]

        if len(subFileList) > 0:
            annotateFilter = AnnotationFilter(
                annotated_resource_type="file", annotated_resource_ids=subFileList
            )
            annotationList = cognite_client.annotations.list(
                limit=-1, filter=annotateFilter
            )

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
    logger: Any,
    cognite_client: CogniteClient,
    assetRootExtId: str,
    entities: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Get Asset used as input to contextualization

    :param cognite_client: Dict of files found based on filter
    :param assetRootExtId: external root asset ID
    :param assetPreProcessing: list of asset preprocessing functions connected to root asset
    :param entities: list of entites found so fare (file names)

    :returns: list of entities
    """

    logger.info(f"Get assets based on asset_subtree_external_ids = {assetRootExtId}")
    assets = cognite_client.assets.list(
        asset_subtree_external_ids=[assetRootExtId], limit=-1
    )

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
            if (
                name is not None and len(name) > 3 and notDymmy
            ):  # ignore system asset names (01, 02, ...)

                nameList.append(name)          
                if name.find('-') > 0 and name.split('-', 1)[0].isnumeric():
                    nameList.append(name.split('-', 1)[1])  
                        
                entities.append(
                    {
                        "externalId": asset.external_id,
                        "name": nameList,
                        "id": asset.id,
                        "type": "asset",
                    }
                )
        except Exception as e:
            logger.error(f"Not able to get entities for asset name: {name}, id {asset.external_id}")
            pass

    return entities



def process_files(
    logger: Any,
    cognite_client: CogniteClient,
    matchTreshold: float,
    entities: List[Dict[str, Any]],
    files: Dict[str, FileMetadata],
    annotationList: Dict[Optional[int], List[Optional[int]]],
    debug: bool,
) -> Tuple[int, int]:
    numAnnotated = 0
    numErrors = 0
    for fileId, file in files.items():
        logger.info(f"Parse and annotate, input file: {fileId}")

        try:
            annotationList = annotationList or {}

            # contextualize, create annotation and get list of matched tags
            entities_name_found, entities_id_found = detect_create_annotation(
                logger, cognite_client, matchTreshold, fileId, entities, annotationList
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
                    convert_date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
                    my_update = (
                        FileMetadataUpdate(id=file.id)
                        .asset_ids.set(assetIdsList)
                        .metadata.add(
                            {file_annotated: convert_date_time, "tags": assetNames}
                        )
                    )
                    updateOrgFiles(logger, cognite_client, my_update, file.external_id)
                except Exception as e:
                    s, r = getattr(e, "message", str(e)), getattr(e, "message", repr(e))
                    logger.warning(
                        f"Not able to update refrence doc : {fileId} - {s}  - {r}"
                    )
                    pass

            else:
                logger.info(
                    f"Converted and created (not upload due to DEBUG) file: {fileId}"
                )
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
                updateOrgFiles(logger, cognite_client, my_update, file.external_id)
            pass

    return numAnnotated, numErrors


def detect_create_annotation(
    logger: Any,
    cognite_client: CogniteClient,
    matchTreshold: float,
    fileId: str,
    entities: List,
    annotationList: Dict,
) -> Tuple[List[Any], List[Any]]:
    """
    Detect tags + files and create annotation for P&ID

    :param logger: loggoer opbject initiated by main module
    :param cognite_client: Dict of files found based on filter
    :param matchTreshold:
    :param fileId:
    :param entities:
    :param annotationList:

    :returns: list of found ID and names in P&ID
    """

    entities_id_found = []
    entities_name_found = []
    createAnnotationList: List[Annotation] = []
    deleteAnnotationList: List[int] = []
    itemNum = 0

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
                logger.warning(
                    f"Retry #{retryNum} - wait before retry - error was: {s}  - {r}"
                )
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

        detectedSytemNum = getSysNums(job.result["items"][0]["annotations"])
        itemNum = len(job.result["items"][0]["annotations"])
        for item in job.result["items"][0]["annotations"]:
            if item["entities"][0]["type"] == "file":
                annotationType = fileAnnotationType
                refType = "fileRef"
                txtValue = item["entities"][0]["orgName"]
            else:
                annotationType = assetAnnotationType
                refType = "assetRef"
                txtValue = item["entities"][0]["name"][0]
                entities_name_found.append(item["entities"][0]["name"][0])
                entities_id_found.append(item["entities"][0]["id"])

            if item["confidence"] >= 0.5:
                tokens = item["text"].split("-")
                
                # logic to create suggestions for annotations if system number is missing 
                # but suggestion matches most frequent system number from P&ID
                if len(tokens) == 2 and item["confidence"] >= matchTreshold and len(item["entities"]) == 1:
                    sysTokenFound = item["entities"][0]["name"][0].split("-")
                    if len(sysTokenFound) == 3:
                        sysNumFound = sysTokenFound[0]
                        if sysNumFound in detectedSytemNum and detectedSytemNum[sysNumFound] / itemNum > 0.5:
                            annotationStatus = annotationStatusAproved
                        else:
                            continue
                    else:
                        continue
                        
                elif item["confidence"] >= matchTreshold and len(item["entities"]) == 1:
                        annotationStatus = annotationStatusAproved

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

                # can oonly create 1000 annotations at the time.
                if len(createAnnotationList) >= 999:
                    cognite_client.annotations.create(createAnnotationList)
                    createAnnotationList = []

        if len(createAnnotationList) > 0:
            cognite_client.annotations.create(createAnnotationList)

        deleteAnnotations(deleteAnnotationList, cognite_client, logger)

        # sort / deduplicate list of names and id
        entities_name_found = list(dict.fromkeys(entities_name_found))
        entities_id_found = list(dict.fromkeys(entities_id_found))

    return entities_name_found, entities_id_found

def  getSysNums(annotations: Any) -> Dict[str, int]:
    """
    Get dict of used system number in P&ID. The dict is used to annotate if system 
    number is missing - but then only annotation of found text is part of most  
    frequent used system number 
    
    :annotations found by context api 

    ::returns: dict of system numbers and number of times used.
    """

    detectedSytemNum = {}

    for item in annotations:
        tokens = item["text"].split("-")
        if len(tokens) == 3:
            sysNum = tokens[0]
            if sysNum in detectedSytemNum:
                detectedSytemNum[sysNum] += 1
            else:
                detectedSytemNum[sysNum] = 1
    
    return detectedSytemNum
    

def getCord(vertices: Dict) -> Tuple[int, int, int, int]:
    """
    Get coordinates for text box based on input from contextualization
    and convert it to coordinates used in annotations.

    :param vertices coordinates from contextualization

    ::returns: coordinates used by annotations.
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


def deleteAnnotations(
    deleteAnnotationList: List, cognite_client: CogniteClient, logger: Any
) -> None:

    """
    Clean up / delete exising annotatoions

    :param deleteAnnotationList: list of annotation IDs to be deleted
    :param logger: loggoer opbject initiated by main module
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
    logger: Any,
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


