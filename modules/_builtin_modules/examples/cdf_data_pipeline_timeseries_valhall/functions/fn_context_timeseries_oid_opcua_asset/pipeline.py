from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun, Row, TimeSeries, TimeSeriesUpdate
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_TS_EXTID,
    TS_CONTEXTUALIZED_METADATA_KEY,
)
from get_resources import (
    get_asset_id_ext_id_mapping,
    get_assets,
    get_matches,
    get_time_series,
    get_ts_list_manual_mapping,
    read_manual_mappings,
)
from write_resources import write_mapping_to_raw


def contextualize_ts_and_asset(client: CogniteClient, config: ContextConfig) -> None:
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
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """
    print("INFO: Initiating contextualization of Time Series and Assets")

    len_good_matches = 0
    len_bad_matches = 0
    manual_mappings = []
    numAsset = -1 if not config.debug else 10_000
    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    for asset_root_ext_id in config.asset_root_ext_ids:
        try:
            manual_mappings = read_manual_mappings(client, config)

            # Start by applying manual mappings - NOTE manual mappings will write over existing mappings
            apply_manual_mappings(client, config, raw_uploader, manual_mappings)
            ts_entities, ts_meta_dict = get_time_series(client, config, manual_mappings)

            # If there is any TS to be contextualized
            if len(ts_entities) > 0:
                asset_entities = get_assets(client, asset_root_ext_id, numAsset)
                if not asset_entities:
                    print(f"WARNING: No assets found for root asset: {asset_root_ext_id}")
                    continue
                match_results = get_matches(client, asset_entities, ts_entities)

                good_matches, bad_matches = select_and_apply_matches(client, config, match_results, ts_meta_dict)
                write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches)
                len_good_matches = len(good_matches)
                len_bad_matches = len(bad_matches)

            msg = (
                f"Contextualization of TS to asset root: {asset_root_ext_id}, num manual mappings: "
                f"{len(manual_mappings)}, num TS contextualized: {len_good_matches}, num TS NOT contextualized : "
                f"{len_bad_matches} (score below {config.match_threshold})"
            )
            print(f"INFO: {msg}")
            client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="success",
                    message=msg,
                )
            )
        except Exception as e:
            msg = f"Contextualization of time series for root asset: {asset_root_ext_id} failed - Message: {e!s}"
            client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="failure",
                    message=shorten(msg, 1000),
                )
            )


def apply_manual_mappings(
    client: CogniteClient, config: ContextConfig, raw_uploader: RawUploadQueue, manual_mappings: list[Row]
) -> None:
    """
    Use input list of manual mappings to update time series with asset ID

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        manual_mappings: list of manual mappings
    """
    if config.debug:
        return
    ts_meta_update_list = []
    mapping = {}
    mapping_dict = {}
    try:
        asset_id_ext_id_mapping = get_asset_id_ext_id_mapping(client, manual_mappings)
        ts_list_manual_mapping = get_ts_list_manual_mapping(client, manual_mappings)

        mapping_dict = {
            mapping[COL_KEY_MAN_MAPPING_TS_EXTID]: mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID]
            for mapping in manual_mappings
        }
        for ts in ts_list_manual_mapping:
            mapping = {}
            ts.metadata = ts.metadata or {}
            ts.metadata[TS_CONTEXTUALIZED_METADATA_KEY] = (
                f"Manual matched from raw: {config.rawdb} / {config.raw_table_manual}"
            )
            asset_id = asset_id_ext_id_mapping[mapping_dict[ts.external_id]]
            ts_metadata_upd = (
                TimeSeriesUpdate(external_id=ts.external_id).asset_id.set(asset_id).metadata.set(ts.metadata)
            )
            ts_meta_update_list.append(ts_metadata_upd)
            mapping[COL_KEY_MAN_MAPPING_TS_EXTID] = ts.external_id
            mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] = mapping_dict[ts.external_id]
            mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_manual, Row(ts.external_id, mapping))

        client.time_series.update(ts_meta_update_list)
        raw_uploader.upload()

    except Exception as e:
        print(f"ERROR: Applying manual mappings. Error: {type(e)}({e})")


def select_and_apply_matches(
    client: CogniteClient,
    config: ContextConfig,
    match_results: list[ContextualizationJob],
    ts_meta_dict: dict[int, Any],
) -> tuple[list[Row], list[Row]]:
    """
    Select and apply matches based on filtering threshold. Matches with score above threshold are updating time series
    with asset ID When matches are updated, metadata property with information about the match is added to time series
    to indicate that it has been matched.

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        match_results: list of matches from entity matching
        ts_meta_dict: dictionary with time series id and metadata

    Returns:
        list of good matches
        list of bad matches
    """
    good_matches = []
    bad_matches = []
    time_series_list = []
    try:
        for match in match_results:
            if match["matches"]:
                if match["matches"][0]["score"] >= config.match_threshold:
                    good_matches.append(add_to_dict(match))
                else:
                    bad_matches.append(add_to_dict(match))
            else:
                bad_matches.append(add_to_dict(match))

        print(f"INFO: Got {len(good_matches)} matches with score >= {config.match_threshold}")
        print(f"INFO: Got {len(bad_matches)} matches with score < {config.match_threshold}")

        # Update time series with matches
        for match in good_matches:
            asset = match["asset_name"]
            score = match["score"]
            ts_id = match["ts_id"]
            asset_id = match["asset_id"]
            metadata = ts_meta_dict[ts_id] or {}

            metadata[TS_CONTEXTUALIZED_METADATA_KEY] = f"Entity matched based on score {score} with asset {asset}"
            ts_elem = TimeSeries(id=ts_id, asset_id=asset_id, metadata=metadata)
            time_series_list.append(ts_elem)

        if not config.debug:
            client.time_series.update(time_series_list)
        return good_matches, bad_matches

    except Exception as e:
        print(f"ERROR: Failed to parse results from entity matching - error: {type(e)}({e})")


def add_to_dict(match: dict[Any]) -> dict[Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """
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
    return {
        "ts_id": source["id"],
        "ts_ext_id": source["external_id"],
        "ts_name": source["org_name"],
        "score": round(score, 2),
        "asset_name": asset_name,
        "asset_ext_id": asset_ext_id,
        "asset_id": asset_id,
    }
