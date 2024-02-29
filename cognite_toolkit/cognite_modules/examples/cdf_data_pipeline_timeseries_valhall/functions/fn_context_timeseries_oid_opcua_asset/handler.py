from __future__ import annotations

import logging
import os
import re
import traceback

from dataclasses import dataclass
from typing import Any

import yaml

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun, Row, TimeSeries, TimeSeriesUpdate
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import RawUploadQueue
from cognite.logger import configure_logger


# defaults
TS_CONTEXTUALIZED_METADATA_KEY = "TS_CONTEXTUALIZED"
ML_MODEL_FEATURE_TYPE = "bigram-combo"
COL_MATCH_KEY = "name"

COL_KEY_MAN_MAPPING_TS_EXTID = "timeSeriesExternalId"  # ExternalID for TS not mapped related to manual mapping
COL_KEY_MAN_MAPPING_ASSET_EXTID = "assetExternalId"  # ExternalID Col name for Asset related to manual mapping
COL_KEY_MAN_CONTEXTUALIZED = "contextualized"  # Col name for if mapping is done for manual mapping

# static variables
FUNCTION_NAME = "TS & Asset contextualization"

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
    time_series_prefix: str
    time_series_data_set_ext_id: str
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
            time_series_prefix=data["timeSeriesPrefix"],
            time_series_data_set_ext_id=data["timeSeriesDataSetExtId"],
            asset_root_ext_ids=data["assetRootExtIds"],
            match_threshold=data["matchThreshold"],
        )


def handle(data: dict, cognite_client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    """
    Function handler for contextualization of Time Series and Assets
    Note that the name in the definition needs to be handle related to CDF Function usage

    Args:
        data: dictionary containing the function input configuration data (by default only the ExtractionPipelineExtId)
        cognite_client: Instance of CogniteClient
        secrets: dictionary containing the function secrets
        function_call_info: dictionary containing the function call information

    Returns:
        dict containing the function output data
    """
    try:
        config = load_config_parameters(cognite_client, data)
        contextualize_ts_and_asset(cognite_client, config)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Function: {FUNCTION_NAME}: failed - Message: {e!r} - {tb}")
        return {
            "error": e.__str__(),
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


def load_config_parameters(cognite_client: CogniteClient, function_data: dict[str, Any]) -> ContextConfig:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.
    Configuration is loaded from the extraction pipeline configuration and the function data.

    Args:
        cognite_client: Instance of CogniteClient
        function_data: dictionary containing the function input configuration data

    Returns:
        ContextConfig object
    """

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
    1. Read RAW table with manual mappings and extract all rows not contextualized
    2. Apply manual mappings from TS to Asset - this will overwrite any existing mapping
    3. Read all time series not matched (or all if runAll is True)
    4. Read all assets
    5. Run ML contextualization to match TS -> Assets
    6. Update TS with mapping
    7. Write results matched (good) not matched (bad) to RAW
    8. Output in good/bad table can then be used in workflow to update manual mappings

    Args:
        cognite_client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process

    Returns:
        None
    """

    logger.info("Initiating contextualization of Time Series and Assets")

    numAsset = -1
    if config.debug:
        numAsset = 10000

    raw_uploader = RawUploadQueue(cdf_client=cognite_client, max_queue_size=500000, trigger_log_level="INFO")

    for asset_root_ext_id in config.asset_root_ext_ids:
        try:

            logger.info(f"Reading manual mapping table from from db: {config.rawdb} table {config.raw_table_manual}")
            manual_mappings = read_manual_mappings(cognite_client, config)

            # Start by applying manual mappings - NOTE manual mappings will write over existing mappings
            logger.info("Applying manual mappings")
            apply_manual_mappings(cognite_client, config, raw_uploader, manual_mappings)

            logger.info(
                f"Read time series for contextualization data set: {config.time_series_data_set_ext_id}, asset root: {asset_root_ext_id} - read and process all = {config.run_all}"
            )
            ts_entities, ts_meta_dict = get_time_series(cognite_client, config, manual_mappings)

            # If there is any TS to be contextualized
            if len(ts_entities) > 0:

                logger.info(f"Get assets based on asset_subtree_external_ids = {asset_root_ext_id}")
                asset_entities = get_assets(cognite_client, asset_root_ext_id, numAsset)

                logger.info(f"Get and run ML model: {ML_MODEL_FEATURE_TYPE}, for matching and TS & Assets")
                mRes = get_matches(cognite_client, asset_entities, ts_entities)

                logger.info("Select and apply matches")
                good_matches, bad_matches = select_and_apply_matches(cognite_client, config, mRes, ts_meta_dict)

                logger.info("Write mapped and unmapped entities to RAW")
                write_mapping_to_raw(cognite_client, config, raw_uploader, good_matches, bad_matches)
                len_good_matches = len(good_matches)
                len_bad_matches = len(bad_matches)

            msg = f"Contextualization of TS to asset root: {asset_root_ext_id}, num manual mappings: {len(manual_mappings)}, num TS contextualized: {len_good_matches}, num TS NOT contextualized : {len_bad_matches} (score below {config.match_threshold})"
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
    """
    Read manual mappings from RAW DB and add to list of manual mappings if not already contextualized

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig

    Returns:
        list of manual mappings or empty list if no mappings are found
    """

    manual_mappings = []
    man_table_true = False

    try:
        table_list = cognite_client.raw.tables.list(config.rawdb, limit=-1)
        for table in table_list:
            if table.name == config.raw_table_manual:
                man_table_true = True
                break

        if man_table_true:
            row_list = cognite_client.raw.rows.list(config.rawdb, config.raw_table_manual, limit=-1)

            for row in row_list:
                if config.run_all or (
                    COL_KEY_MAN_CONTEXTUALIZED not in row.columns or row.columns[COL_KEY_MAN_CONTEXTUALIZED] is not True
                ):
                    strip_value_rows = {}
                    for key, value in row.columns.items():

                        if isinstance(value, str):
                            strip_value_rows[key.strip()] = value.strip()
                        else:
                            strip_value_rows[key.strip()] = value

                    manual_mappings.append(strip_value_rows)

            logger.info(f"Number of manual mappings: {len(manual_mappings)}")

    except Exception as e:
        logger.error(f"Read manual mappings. Error: {e}")

    return manual_mappings


def apply_manual_mappings(
    cognite_client: CogniteClient, config: ContextConfig, raw_uploader: RawUploadQueue, manual_mappings: list[Row]
) -> None:
    """
    Use input list of manual mappings to update time series with asset ID

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        manual_mappings: list of manual mappings

    Returns:
        None
    """

    ts_meta_update_list = []
    mapping = {}
    mapping_dict = {}

    try:
        asset_id_ext_id_mapping = get_asset_id_ext_id_mapping(cognite_client, manual_mappings)
        ts_list_manual_mapping = get_ts_list_manual_mapping(cognite_client, manual_mappings)

        mapping_dict = {
            mapping[COL_KEY_MAN_MAPPING_TS_EXTID]: mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID]
            for mapping in manual_mappings
        }

        for time_series in ts_list_manual_mapping:
            mapping = {}
            if time_series.metadata is None:
                time_series.metadata = {}

            time_series.metadata[
                TS_CONTEXTUALIZED_METADATA_KEY
            ] = f"Manual matched from raw: {config.rawdb} / {config.raw_table_manual}"

            asset_id = asset_id_ext_id_mapping[mapping_dict[time_series.external_id]]

            ts_metadata_upd = (
                TimeSeriesUpdate(external_id=time_series.external_id)
                .asset_id.set(asset_id)
                .metadata.set(time_series.metadata)
            )

            ts_meta_update_list.append(ts_metadata_upd)

            if not config.debug:
                mapping[COL_KEY_MAN_MAPPING_TS_EXTID] = time_series.external_id
                mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] = mapping_dict[time_series.external_id]
                mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
                raw_uploader.add_to_upload_queue(
                    config.rawdb, config.raw_table_manual, Row(time_series.external_id, mapping)
                )

        if not config.debug:
            cognite_client.time_series.update(ts_meta_update_list)

            # Update row in RAW
            raw_uploader.upload()

    except Exception as e:
        logger.error(f"[FAILED] Applying manual mappings. Error: {e}")


def get_asset_id_ext_id_mapping(cognite_client: CogniteClient, manual_mappings: list[Row]) -> dict[str, int]:
    """
    Read assets specified in manual mapping input based on external ID and find the corresponding asset internal ID
    Internal ID is used to update time series with asset ID

    Args:
        cognite_client: Instance of CogniteClient
        manual_mappings: list of manual mappings

    Returns:
        dictionary with asset external id as key and asset id as value

    """

    asset_ext_id_mapping = {}
    try:

        asset_ext_id_list = [mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] for mapping in manual_mappings]
        asset_ext_id_list_no_dups = list(set(asset_ext_id_list))
        asset_list = cognite_client.assets.retrieve_multiple(external_ids=asset_ext_id_list_no_dups)

        asset_ext_id_mapping = {asset.external_id: asset.id for asset in asset_list}

    except Exception as e:
        logger.error(f"Not able read list of assets from {manual_mappings} - error: {e}")

    return asset_ext_id_mapping


def get_ts_list_manual_mapping(cognite_client: CogniteClient, manual_mappings: list[Row]) -> list[TimeSeries]:
    """
    Read time series related to external time series ID specified in manual mapping input

    Args:
        cognite_client: Instance of CogniteClient
        manual_mappings: list of manual mappings

    Returns:
        list of TimeSeries
    """

    ts_list_manual_mapping = []

    try:
        ts_ext_id_list = [mapping[COL_KEY_MAN_MAPPING_TS_EXTID] for mapping in manual_mappings]

        ts_list_manual_mapping = cognite_client.time_series.retrieve_multiple(external_ids=ts_ext_id_list)

        if len(ts_list_manual_mapping) != len(ts_ext_id_list):
            raise ValueError(
                f"Number of time series found {len(ts_list_manual_mapping)} does not match number of time series in input list {len(ts_ext_id_list)}"
            )

    except Exception as e:
        logger.error(f"Not able read list of time series from {manual_mappings} - error: {e}")

    return ts_list_manual_mapping


def get_time_series(
    cognite_client: CogniteClient, config: ContextConfig, manual_matches: list[Row]
) -> tuple[list[dict[str, Any]], list[TimeSeries]]:
    """
    Read time series based on root ASSET id
    Read all if config property readAll = True, else only read time series not contextualized ( connected to asset)

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig
        manual_matches: list of manual mappings

    Returns:
        list of entities
        list of dict with time series id and metadata
    """

    meta_ts_update: list[TimeSeries] = []
    entities: list[dict[str, Any]] = []
    ts_meta_dict = {}

    try:
        ts_list = cognite_client.time_series.list(
            data_set_external_ids=[config.time_series_data_set_ext_id], external_id_prefix=config.time_series_prefix
        )

        # get list of time series in manual mappings to exclude from matching
        ts_ext_id_manual_match_list = [m_match[COL_KEY_MAN_MAPPING_TS_EXTID] for m_match in manual_matches]

        for ts in ts_list:

            # if manual match exists, skip matching based on entity matching
            if ts.external_id in ts_ext_id_manual_match_list:
                continue

            if TS_CONTEXTUALIZED_METADATA_KEY is not None and TS_CONTEXTUALIZED_METADATA_KEY not in (ts.metadata or {}):
                if ts.external_id is not None:
                    entities = get_ts_entities(ts, entities)

            # if run all - remove metadata element from last annotation
            elif config.run_all:
                if not config.debug and TS_CONTEXTUALIZED_METADATA_KEY is not None:
                    ts_meta_update = TimeSeriesUpdate(external_id=ts.external_id).metadata.remove(
                        [TS_CONTEXTUALIZED_METADATA_KEY]
                    )
                    meta_ts_update.append(ts_meta_update)
                if ts.external_id is not None:
                    entities = get_ts_entities(ts, entities)

            ts_meta_dict[ts.id] = ts.metadata

            if config.debug:
                break

        if len(meta_ts_update) > 0:
            cognite_client.time_series.update(meta_ts_update)

    except Exception as e:
        logger.error(
            f"Not able to get entities for time series in data set: {config.time_series_data_set_ext_id} with prefix: {config.time_series_prefix}- error: {e}"
        )

    return entities, ts_meta_dict


def get_ts_entities(ts: TimeSeries, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    process time series metadata and create an entity used as input to contextualization

    Args:
        ts: Instance of TimeSeries
        entities: already processed entities

    Returns:
        list of entities
    """

    if ts.name is None:
        logger.warning(f"No name found for time series with external ID: {ts.external_id}, and metadata: {ts}")
        return entities

    # build list with possible file name variations used in P&ID to refer to other P&ID
    split_name = re.split("[,._ \\-!?:]+", ts.name)

    modName = ts.name[len(split_name[0]) + 1 :]  # remove prefix
    modName = modName[: -(len(split_name[-1]) + 1)]  # remove postfix

    # add entities for files used to match between file references in P&ID to other files
    entities.append({"id": ts.id, "name": modName, "external_id": ts.external_id, "org_name": ts.name, "type": "ts"})

    return entities


def get_assets(cognite_client: CogniteClient, asset_root_ext_id: str, numAsset: int) -> list[dict[str, Any]]:
    """
    Get Asset used as input to contextualization and build list of entities

    Args:
        cognite_client: Instance of CogniteClient
        asset_root_ext_id: external root asset ID
        numAsset : number of assets to read

    Returns:
        list of entities
    """

    entities: list[dict[str, Any]] = []

    try:

        assets = cognite_client.assets.list(asset_subtree_external_ids=[asset_root_ext_id], limit=numAsset)

        # clean up dummy tags and system numbers
        for asset in assets:
            name = asset.name

            # Exclude if Description in metadata contains "DUMMY TAG"
            if (
                asset.metadata is not None
                and "Description" in asset.metadata
                and "DUMMY TAG" in asset.metadata.get("Description", "").upper()
            ):
                continue

            # Split name - and if a system number is used also add name without system number to list
            split_name = re.split("[,._ \\-:]+", name)

            # ignore if no value in name and if system asset names (01, 02, ...) and if at least 3 tokens in name
            if name is not None and len(name) > 3 and len(split_name) >= 3:

                # Do any manual updates changes to name to clean up and make it easier to match
                modName = name

                entities.append(
                    {
                        "id": asset.id,
                        "name": modName,
                        "external_id": asset.external_id,
                        "org_name": asset.name,
                        "type": "asset",
                    }
                )

    except Exception as e:
        logger.error(f"Not able to get entities for asset extId root: {asset_root_ext_id} - error: {e}")

    return entities


def get_matches(
    cognite_client: CogniteClient, match_to: list[dict[str, Any]], match_from: list[dict[str, Any]]
) -> list[ContextualizationJob]:
    """
    Create / Update entity matching model and run job to get matches

    Args:
        cognite_client: Instance of CogniteClient
        match_to: list of entities to match to (target)
        match_from: list of entities to match from (source)

    Returns:
        list of matches
    """

    try:

        model = cognite_client.entity_matching.fit(
            sources=match_from,
            targets=match_to,
            match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
            feature_type="bigram-combo",
        )

        logger.info("Run prediction based on model")
        job = model.predict(sources=match_from, targets=match_to, num_matches=1)
        matches = job.result
        return matches["items"]

    except Exception as e:
        logger.error(f"Failed to get matching model and run prediction - error: {e}")


def select_and_apply_matches(
    cognite_client: CogniteClient, config: ContextConfig, mRes: list[ContextualizationJob], ts_meta_dict: dict[int, Any]
) -> tuple[list[Row], list[Row]]:
    """
    Select and apply matches based on filtering threshold. Matches with score above threshold are updating time series with asset ID
    When matches are updated, metadata property with information about the match is added to time series to indicate that it has been matched

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig
        mRes: list of matches from entity matching
        ts_meta_dict: dictionary with time series id and metadata

    Returns:
        list of good matches
        list of bad matches
    """

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

        # Update time series with matches
        for match in good_matches:
            asset = match["asset_name"]
            score = match["score"]
            ts_id = match["ts_id"]
            asset_id = match["asset_id"]
            metadata = ts_meta_dict[ts_id]

            if metadata is None:
                metadata = {}

            metadata[TS_CONTEXTUALIZED_METADATA_KEY] = f"Entity matched based on score {score} with asset {asset}"

            tsElem = TimeSeries(id=ts_id, asset_id=asset_id, metadata=metadata)

            time_series_list.append(tsElem)

        if not config.debug:
            cognite_client.time_series.update(time_series_list)

        return good_matches, bad_matches

    except Exception as e:
        logger.error(f"Failed to parse results from entity matching - error: {e}")


def add_to_dict(match: dict[Any]) -> dict[Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """

    row = {}

    try:
        source = match["source"]

        if len(match["matches"]) > 0:
            target = match["matches"][0]["target"]
            score = match["matches"][0]["score"]
            asset_name = target["org_name"]
            asset_id = target["id"]
            asset_ext_id = target["external_id"]
        else:
            score = 0
            asset_name = "_no_match_"
            asset_id = None
            asset_ext_id = None

        row = {
            "ts_id": source["id"],
            "ts_ext_id": source["external_id"],
            "ts_name": source["org_name"],
            "score": round(score, 2),
            "asset_name": asset_name,
            "asset_ext_id": asset_ext_id,
            "asset_id": asset_id,
        }

    except Exception as e:
        logger.error(f"ERROR: Not able to parse return object: {match} - error: {e}")

    return row


def write_mapping_to_raw(
    cognite_client: CogniteClient,
    config: ContextConfig,
    raw_uploader: RawUploadQueue,
    good_matches: list[Row],
    bad_matches: list[Row],
) -> None:
    """
    Write matching results to RAW DB

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        good_matches: list of good matches
        bad_matches: list of bad matches

    Returns:
        None
    """

    logger.info(f"Clean up BAD table: {config.rawdb}/{config.raw_table_bad} before writing new status")
    try:
        cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_bad])
    except Exception:
        pass  # no table to delete

    # if reset mapping, clean up good matches in table
    if config.run_all and not config.debug:
        logger.info(
            f"ResetMapping - Cleaning up GOOD table: {config.rawdb}/{config.raw_table_good} before writing new status"
        )
        try:
            cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_good])
        except Exception:
            pass  # no table to delete

    for match in good_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["ts_ext_id"], match))
    logger.info(f"Added {len(good_matches)} to {config.rawdb}/{config.raw_table_good}")

    for not_match in bad_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["ts_ext_id"], not_match))
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
