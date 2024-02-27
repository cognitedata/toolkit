from __future__ import annotations

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
    ExtractionPipelineRun,
    ContextualizationJob,
    TimeSeriesUpdate,
    TimeSeries,
    Row
)

from cognite.extractorutils.uploader import RawUploadQueue

from cognite.client.data_classes.contextualization import DiagramDetectResults
from cognite.client.exceptions import CogniteAPIError
from cognite.logger import configure_logger

# defaults
TS_CONTEXTUALIZED_METADATA_KEY = "TS_CONTEXTUALIZED"
COL_MATCH_KEY = "name"

COL_KEY_MAN_MAPPING_TS_EXTID = "TS ExtId"  # ExternalID for TS not mapped related to manual mapping
COL_KEY_MAN_MAPPING_ASSET_EXTID  = "Asset ExtId"  # ExternalID Col name for Assset related to manual mapping
COL_KEY_MAN_MAPPED = "Mapped"  # Col name for if mapping is done for manual mapping

# static variables
FUNCTION_NAME = "TS & Asset contextualization"

# logging the output
# Configure application logger (only done ONCE):
configure_logger(logger_name="func", log_json=False, log_level="INFO")

# The following line must be added to all python modules (after imports):
logger = logging.getLogger(f"func.{__name__}")
logger.info("---------------------------------------START--------------------------------------------")


@dataclass
class ContextConfig:
    extraction_pipeline_ext_id: str
    debug: bool
    run_all: bool
    rawdb: str
    raw_table_good: str
    raw_table_bad: str
    raw_table_manual: str
    asset_root_ext_ids: list[str]
    match_threshold: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> ContextConfig:
        return cls(
            extraction_pipeline_ext_id=data["ExtractionPipelineExtId"],
            debug=data["debug"],
            run_all=data["runAll"],
            rawdb=data["rawdb"],
            raw_table_good=data["rawTableGood"],
            raw_table_bad=data["rawTableBad"],
            raw_table_manual=data["rawTableManual"],
            time_sereis_prefix=data["timeSereisPrefix"]
            time_series_data_set_ext_id=data["timeSeriesDataSetExtId"],
            asset_root_ext_ids=data["assetRootExtIds"],
            match_threshold=data["matchThreshold"],
        )



@dataclass
class Entity:
    external_id: str
    org_name: str
    name: list[str]
    id: int
    type: str

    def dump(self) -> dict[str, Any]:
        return {
            "externalId": self.external_id,
            "orgName": self.org_name,
            "name": self.name,
            "id": self.id,
            "type": self.type,
        }


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    msg = ""
    logger.info("[STARTING] Extracting input data")

    try:
        config = load_config_parameters(client, data)
        logger.info("[FINISHED] Extracting input parameters")
        contextualize_ts_and_asset(client, config)
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {FUNCTION_NAME}: failed - Message: {e!r} - {tb}"
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


def load_config_parameters(cognite_client: CogniteClient, function_data: dict[str, Any]) -> ContextConfig:
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
    return ContextConfig.load(data)


def contextualize_ts_and_asset(cognite_client: CogniteClient, config: ContextConfig) -> None:
    """
    Read configuration and start process by
    1. Read all time series not matched (or all if runAll is True)
    2. Read all assets 
    3. Read and apply any defined manual matches defined in RAW table (table: contextualization_manual_input) 
    4. Run ML contextualization to match TS -> Assets 
    5. Update TS with mapping 
    6. Write results matched (good) not matched (bad) to RAW

    Args:
        cognite_client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process

    """

    logger.info("Initiating contextualization of Time Series and Assets")

    
    numAsset = -1
    if config.debug:
        numAsset = 10000

    raw_uploader = RawUploadQueue(cdf_client=cognite_client, max_queue_size=500000, trigger_log_level="INFO")


    for asset_root_ext_id in config.asset_root_ext_ids:
        try:
                       
            logger.info(f"Reading manual mapping table from from db: {config.rawdb} table {config.raw_table_manual}")
            manual_mappings = read_manual_mappings(cognite_client, config, raw_uploader)
 
            # Start by applying manual mappings - NOTE manual mappings will write over existing mappings
            logger.info("Applying manual mappings")
            apply_manual_mappings(cognite_client, raw_uploader, manual_mappings)

            logger.info(f"Read time series for contextualization data set: {config.doc_data_set_ext_id}, asset root: {asset_root_ext_id} - read and process all = {config.run_all}")
            ts_entities, ts_list = get_time_series(
                cognite_client,
                asset_root_ext_id,
                config,
            )
  
            if len(ts_entities) > 0:

                logger.info(f"Get assets based on asset_subtree_external_ids = {asset_root_ext_id}")
                asset_entities = get_assets(cognite_client, asset_root_ext_id, numAsset)
                
                logger.info("Get and run model for matching and TS & Assets")
                mRes = get_matches(cognite_client, asset_entities, ts_entities)
                
                logger.info("# Select and apply matches")
                good_matches, bad_matches = select_and_apply_matches(cognite_client, config, mRes, ts_list, manual_mappings)

                logger.info("# Write mapped and unmapped entities to RAW")
                write_mapping_to_raw(cognite_client, config, raw_uploader, good_matches, bad_matches)
                len_good_matches = len(good_matches)
                len_bad_matches = len(bad_matches)


            msg = f"Contextualization of time series for asset: {asset_root_ext_id} number of time series contextualized: {len_good_matches} - time series not contextualized (low confidence score): {len_bad_matches}"
            logger.info(msg)
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="success",
                    message=msg,
                )
            )

        except (CogniteAPIError, Exception) as e:
            msg = f"Contextualization of time series for root asset: {asset_root_ext_id} failed - Message: {e!s}"
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


def read_manual_mappings(cognite_client: CogniteClient, config: ContextConfig) -> list[Row]:
 

    manual_mappings = []
    man_table_true = False

    try:
        table_list = cognite_client.raw.tables.list(config.rawdb, limit=-1)
        for table in table_list:
            if table.name == config.raw_table_manual:
                man_table_true = True
                break

        if man_table_true:
            row_list = cognite_client.raw.rows.list(config.rawdb,config.raw_table_manual, limit=-1)

            for row in row_list:
                if (
                    COL_KEY_MAN_MAPPED in row.columns
                    and row.columns[COL_KEY_MAN_MAPPED] != "True"
                ):
                    manual_mappings.append(row.columns)

            logger.info(f"[INFO] Number of mappings: {len(manual_mappings)}")

    except Exception as e:
        logger.error(f"[FAILED] Read manual mappings. Error: {e}")


    return manual_mappings


#
# Read manual mapping table from RAW
#
def apply_manual_mappings(cognite_client: CogniteClient, config: ContextConfig, raw_uploader: RawUploadQueue, manual_mappings: list[Row]) -> None:

    ts_meta_update_list = []
    mapping = {}
    mapping_dict = {}
    
    try:
        asset_id_ext_id_mappping = get_asset_id_ext_id_mappping(cognite_client, manual_mappings)
        ts_list = get_ts_list(cognite_client, manual_mappings) 


        for mapping in manual_mappings:
            mapping_dict[mapping[COL_KEY_MAN_MAPPING_TS_EXTID]] = mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID]

        for time_series in ts_list:
            if time_series.metadata is None:
                metadata = {}

            metadata[TS_CONTEXTUALIZED_METADATA_KEY] = f"Manual matched based on input from db: {config.rawdb} table {config.raw_table_manual}"

            asset_id = asset_id_ext_id_mappping[mapping_dict[time_series.external_id]]
      
            ts_metadata_upd = (
                TimeSeriesUpdate(external_id=time_series.external_id)
                .asset_id.set(asset_id)
                .metadata.set(metadata)
            )

            ts_meta_update_list.append(ts_metadata_upd)

            if not config.debug:
                mapping[COL_KEY_MAN_MAPPING_TS_EXTID] = time_series.external_id
                mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] = mapping_dict[time_series.external_id]
                mapping[COL_KEY_MAN_MAPPED] = "True"
                raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_manual, Row(time_series.external_id, mapping))

        if not config.debug:
            cognite_client.time_series.update(ts_meta_update_list) 

            # Update row in RAW
            raw_uploader.upload()

    except Exception as e:
        logger.error(f"[FAILED] Applying manual mappings. Error: {e}")
        
        

def get_asset_id_ext_id_mappping(
    cognite_client: CogniteClient, 
    manual_mappings: list[Row]
) -> dict[str, int]:

    asset_ext_id_mappping = {}
    asset_ext_id_list = {}
    try:
        for mapping in manual_mappings:
            asset_ext_id_list.append(mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID])
        
        asset_list = cognite_client.assets.retrieve_multiple(external_ids=asset_ext_id_list)
        for asset in asset_list:
            asset_ext_id_mappping[asset.external_id] = asset.id

    except Exception as e:
        logger.error(f"Not able read list of assets from {manual_mappings} - error: {e}")

    return asset_ext_id_mappping

def get_ts_list(
    cognite_client: CogniteClient, 
    manual_mappings: list[Row]
) -> list[str]:

    ts_list = []
    ts_ext_id_list = []
    
    try:
        
        for mapping in manual_mappings:
            ts_ext_id_list.append(mapping[COL_KEY_MAN_MAPPING_TS_EXTID])
        
        ts_list = cognite_client.time_series.retrieve_multiple(external_ids=ts_ext_id_list)

    except Exception as e:
        logger.error(f"Not able read list of time series from {manual_mappings} - error: {e}")
        
    return ts_list
                           
    
def get_time_series(
    cognite_client: CogniteClient,
    asset_root_ext_id: str,
    config: ContextConfig,
) -> tuple[list[Entity], list[TimeSeries]]:
    """
    Read time sereis based on root ASSET id
    Read all if config .readAll = True, else only read time sereis not contextualized ( connected to asset)

    :returns: dict of time series to process 
    
    """

    meta_ts_update: list[TimeSeries] = []
    entities: list[Entity] = []

    logger.info(
        f"Get time series to contextualize from data set: {config.doc_data_set_ext_id}, asset root: {asset_root_ext_id} "
    )

    ts_list = cognite_client.time_series.list(
        asset_subtree_external_ids=[asset_root_ext_id],
        data_set_external_ids=[config.time_series_data_set_ext_id]
    )

    for ts in ts_list:
        if TS_CONTEXTUALIZED_METADATA_KEY is not None and TS_CONTEXTUALIZED_METADATA_KEY not in (ts.metadata or {}):
            if ts.external_id is not None:
                entities = get_ts_entities(entities, ts)

        # if run all - remove metadata element from last annotation
        elif config.run_all:
            if not config.debug and TS_CONTEXTUALIZED_METADATA_KEY is not None:
                ts_meta_update = TimeSeriesUpdate(external_id=ts.external_id).metadata.remove(
                    [TS_CONTEXTUALIZED_METADATA_KEY]
                )
                meta_ts_update.append(ts_meta_update)
            if ts.external_id is not None:
                entities = get_ts_entities(entities, ts)
        if config.debug:
            break

    if len(meta_ts_update) > 0:
        cognite_client.files.update(meta_ts_update)

    return entities, ts_list



def get_ts_entities(ts: TimeSeries, entities: list[Entity]) -> list[Entity]:
    """
    Loop found time series and create a list of entities used for matching against assets

    Args:
        ts_to_process: Dict of time series found based on filter
    """

    ts_list = []
    if ts.name is None:
        logger.warning(f"No name found for time series with external ID: {ts.external_id}, and metadata: {ts}")
        return entities

    # build list with possible file name variations used in P&ID to refer to other P&ID
    split_name = re.split("[,._ \\-!?:]+", ts.name)

    core_name = ""
    next_name = ""
    for name in reversed(split_name):
        if core_name == "":
            idx = ts.name.find(name)
            core_name = ts.name[: idx - 1]
            ts_list.append(core_name)
        else:
            idx = core_name.find(name + next_name)
            if idx != 0:
                ctx_name = core_name[idx:]
                if next_name != "":  # Ignore first part of name in matching
                    ts_list.append(ctx_name)
                next_name = core_name[idx - 1 :]

    # add entities for files used to match between file references in P&ID to other files
    entities.append(
        Entity(
            external_id=ts.external_id,
            org_name=ts.name,
            name=ts_list,
            id=ts.id,
            type="ts",
        )
    )

    return entities




def get_assets(cognite_client: CogniteClient, asset_root_ext_id: str, numAsset: int) -> None:
    """Get Asset used as input to contextualization
    Args:
        cognite_client: Instance of CogniteClient
        asset_root_ext_id: external root asset ID
        numAsset : number of assets to read

    Returns:
        list of entities
    """
    entities: list[Entity] = []
    
    try:
    
        assets = cognite_client.assets.list(asset_subtree_external_ids=[asset_root_ext_id], limit=numAsset)

        # clean up dummy tags and system numbers
        for asset in assets:
            name = asset.name
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
    except Exception as e:
        logger.error(f"Not able to get entities for asset name: {name}, id {asset.external_id}- error: {e}")

    return entities


#
# Create / Update model and run job to get matches
#
def get_matches(
    cognite_client: CogniteClient, 
    match_to: list[Entity], 
    match_from:list[Entity]
) -> list[ContextualizationJob]:
    
    try:
        
        model = cognite_client.entity_matching.fit(
            sources=match_from,
            targets=match_to,
            match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
            feature_type="bigram",
        )
        
        logger.info("Run prediction based on model")
        job = model.predict(sources=match_from, targets=match_to, num_matches=1)
        matches = job.result
        return matches["items"]
    
    except Exception as e:
        logger.error(f"Failed to get matching model and run prediction - error: {e}")


#
# Select and apply matches bsed on filtering treshold
#
def select_and_apply_matches(
    cognite_client: CogniteClient, 
    config: ContextConfig,
    mRes: list[ContextualizationJob], 
    ts_list: list[TimeSeries],
    manual_matches: list[Row]
) -> tuple[list[Row], list[Row]]:

    good_matches = []
    bad_matches = []
    time_series_list = []
    
    try:
        for match in mRes:
            if len(match["matches"]) > 0:
                if match["matches"][0]["score"] >= config.match_threshold:
                    good_matches.append(add_to_dict(match))
                else:
                    bad_matches.append(add_to_dict(match))
            else:
                bad_matches.append(add_to_dict(match))
                continue

        logger.info(f"Got {len(good_matches)} matches with score >= {config.match_threshold}")
        logger.info(f"Got {len(bad_matches)} matches with score < {config.match_threshold}")
    
        for match in good_matches:
            asset     = match['asset_name']
            score     = match['score']
            ts_id     = match['ts_id']
            ts_ext_id = match['ts_ext_id']
            asset_id  = match['assetId']
            metadata  = ts_list[ts_id]    

            # if manual match exists, skip
            if ts_ext_id in manual_matches:
                continue
            
            if metadata == None:
                metadata = {}

            metadata[TS_CONTEXTUALIZED_METADATA_KEY] = f"Entity matched based on score {score} with asset {asset}"
            
            tsElem = TimeSeries(id = ts_id,
                                asset_id = asset_id,
                                metadata = metadata)

            time_series_list.append(tsElem)

        if not config.debug:
            cognite_client.time_series.update(time_series_list)
        
        return good_matches, bad_matches

    except Exception as e:
        logger.error(f"Failed to parse results from entity matching - error: {e}")




def add_to_dict(
    match: dict[Any]
)-> dict[Any]:

    row = {}

    try:
        m_from = match['matchFrom']

        if len(match['matches']) > 0:
            m_to       = match['matches'][0]['matchTo']
            score     = match['matches'][0]["score"]
            asset_name = m_to["orgName"] 
            asset_id   = m_to["id"]
            asset_ext_id = m_to["externalId"]
        else:
            score     = 0
            asset_name = "_no_match_" 
            asset_id   = None
            asset_ext_id = None
            
        row = {"ts_id"           : m_from["id"], 
               "ts_ext_id"       : m_from["externalId"], 
               "ts_name"         : m_from["orgName"], 
               "score"           : score,
               "asset_name"      : asset_name, 
               "asset_ext_id"    : asset_ext_id,
               "asset_id"        : asset_id}

    except Exception as e:
        logger.error(f"ERROR: Not able to parse retrun object: {match} - error: {e}")

    return row


#
# Write matching results to RAW DB
#
def write_mapping_to_raw(
    cognite_client: CogniteClient, 
    config: ContextConfig,
    raw_uploader: RawUploadQueue,
    good_matches: list[Row], 
    bad_matches: list[Row]
) -> None:
    
    logger.info(f"Clean up BAD table: {config.rawdb}/{config.raw_table_bad} before wrting new status")
    try:
        cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_bad])
    except Exception:
        pass  # no tabe to delete

    # if reset mapping, clean up good matches in table
    if config.run_all and not config.debug:
        logger.info(f"ResetMapping - Cleaning up GOOD table: {config.rawdb}/{config.raw_table_good} before wrting new status")
        try:
            cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_good])
        except Exception:
            pass  # no tabe to delete

    for match in good_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["opcuaAssetExtId"], match))
    logger.info(f"Added {len(good_matches)} to {config.rawdb}/{config.raw_table_good}")
    
    for not_match in bad_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["opcuaAssetExtId"], not_match))
    logger.info(f"Added {len(bad_matches)} to {config.rawdb}/{config.raw_table_bad}")
    
    # Upload any remaining RAW cols in queue
    raw_uploader.upload()



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
        "ExtractionPipelineExtId": "ep_ctx_timeseries_oid_opcua_asset",
    }

    # Test function handler
    handle(data, client, secrets, function_call_info)


if __name__ == "__main__":
    main()
