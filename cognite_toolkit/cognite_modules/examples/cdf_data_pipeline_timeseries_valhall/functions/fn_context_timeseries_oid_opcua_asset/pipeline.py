from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun, Row, TimeSeries, TimeSeriesUpdate
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_TS_EXTID,
    ML_MODEL_FEATURE_TYPE,
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
    """
    print("INFO: Initiating contextualization of Time Series and Assets")

    len_good_matches = 0
    len_bad_matches = 0
    manual_mappings = []

    numAsset = -1 if not config.debug else 10_000

    raw_uploader = RawUploadQueue(cdf_client=cognite_client, max_queue_size=500_000, trigger_log_level="INFO")

    for asset_root_ext_id in config.asset_root_ext_ids:
        try:
            print(f"INFO: Reading manual mapping table from from db: {config.rawdb} table {config.raw_table_manual}")
            manual_mappings = read_manual_mappings(cognite_client, config)

            # Start by applying manual mappings - NOTE manual mappings will write over existing mappings
            print("INFO: Applying manual mappings")
            apply_manual_mappings(cognite_client, config, raw_uploader, manual_mappings)

            print(
                f"INFO: Read time series for contextualization data set: {config.time_series_data_set_ext_id}, "
                f"asset root: {asset_root_ext_id} - read and process all = {config.run_all}"
            )
            ts_entities, ts_meta_dict = get_time_series(cognite_client, config, manual_mappings)

            # If there is any TS to be contextualized
            if len(ts_entities) > 0:

                print(f"INFO: Get assets based on asset_subtree_external_ids = {asset_root_ext_id}")
                asset_entities = get_assets(cognite_client, asset_root_ext_id, numAsset)

                print(f"INFO: Get and run ML model: {ML_MODEL_FEATURE_TYPE}, for matching and TS & Assets")
                mRes = get_matches(cognite_client, asset_entities, ts_entities)

                print("INFO: Select and apply matches")
                good_matches, bad_matches = select_and_apply_matches(cognite_client, config, mRes, ts_meta_dict)

                print("INFO: Write mapped and unmapped entities to RAW")
                write_mapping_to_raw(cognite_client, config, raw_uploader, good_matches, bad_matches)
                len_good_matches = len(good_matches)
                len_bad_matches = len(bad_matches)

            msg = f"Contextualization of TS to asset root: {asset_root_ext_id}, num manual mappings: {len(manual_mappings)}, num TS contextualized: {len_good_matches}, num TS NOT contextualized : {len_bad_matches} (score below {config.match_threshold})"
            print(f"INFO: {msg}")
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="success",
                    message=msg,
                )
            )

        except Exception as e:
            msg = f"Contextualization of time series for root asset: {asset_root_ext_id} failed - Message: {e!s}"
            if len(msg) > 1000:
                msg = msg[0:995] + "..."
            cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="failure",
                    message=msg,
                )
            )


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

            time_series.metadata[TS_CONTEXTUALIZED_METADATA_KEY] = (
                f"Manual matched from raw: {config.rawdb} / {config.raw_table_manual}"
            )

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
        print(f"ERROR: [FAILED] Applying manual mappings. Error: {e}")


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
            cognite_client.time_series.update(time_series_list)

        return good_matches, bad_matches

    except Exception as e:
        print(f"ERROR: Failed to parse results from entity matching - error: {e}")


def add_to_dict(match: dict[Any]) -> dict[Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """
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

        return {
            "ts_id": source["id"],
            "ts_ext_id": source["external_id"],
            "ts_name": source["org_name"],
            "score": round(score, 2),
            "asset_name": asset_name,
            "asset_ext_id": asset_ext_id,
            "asset_id": asset_id,
        }
    except Exception as e:
        print(f"ERROR: Not able to parse return object: {match} - error: {e}")
        return {}
