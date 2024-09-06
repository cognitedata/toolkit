from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, ContextualizationJob, Row, TimeSeries, TimeSeriesUpdate

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_TS_EXTID,
    COL_MATCH_KEY,
    ML_MODEL_FEATURE_TYPE,
    TS_CONTEXTUALIZED_METADATA_KEY,
)


def manual_table_exists(client: CogniteClient, config: str) -> bool:
    tables = client.raw.tables.list(config.rawdb, limit=None)
    return any(tbl.name == config.raw_table_manual for tbl in tables)


def read_manual_mappings(client: CogniteClient, config: ContextConfig) -> list[Row]:
    """
    Read manual mappings from RAW DB and add to list of manual mappings if not already contextualized

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig

    Returns:
        list of manual mappings or empty list if no mappings are found
    """
    manual_mappings = []
    seen_mappings = set()
    try:
        if not manual_table_exists(client, config):
            return manual_mappings

        row_list = client.raw.rows.list(config.rawdb, config.raw_table_manual, limit=-1)
        for row in row_list:
            if not (
                config.run_all
                or COL_KEY_MAN_CONTEXTUALIZED not in row.columns
                or row.columns[COL_KEY_MAN_CONTEXTUALIZED] is not True
            ):
                continue

            # Make sure we don't add duplicate TS external IDs
            ts_xid = row.columns[COL_KEY_MAN_MAPPING_TS_EXTID].strip()
            if ts_xid not in seen_mappings:
                seen_mappings.add(ts_xid)
                manual_mappings.append(
                    {
                        COL_KEY_MAN_MAPPING_TS_EXTID: ts_xid,
                        COL_KEY_MAN_MAPPING_ASSET_EXTID: row.columns[COL_KEY_MAN_MAPPING_ASSET_EXTID].strip(),
                    }
                )

        print(f"INFO: Number of manual mappings: {len(manual_mappings)}")

    except Exception as e:
        print(f"ERROR: Read manual mappings. Error: {type(e)}({e})")

    return manual_mappings


def get_asset_id_ext_id_mapping(client: CogniteClient, manual_mappings: list[Row]) -> dict[str, int]:
    """
    Read assets specified in manual mapping input based on external ID and find the corresponding asset internal ID
    Internal ID is used to update time series with asset ID

    Args:
        client: Instance of CogniteClient
        manual_mappings: list of manual mappings

    Returns:
        dictionary with asset external id as key and asset id as value

    """
    try:
        asset_ext_id_list = [mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] for mapping in manual_mappings]
        asset_list = client.assets.retrieve_multiple(external_ids=list(set(asset_ext_id_list)))
        return {asset.external_id: asset.id for asset in asset_list}

    except Exception as e:
        print(f"ERROR: Not able read list of assets from {manual_mappings}. Error: {type(e)}({e})")
        return {}


def get_ts_list_manual_mapping(client: CogniteClient, manual_mappings: list[Row]) -> list[TimeSeries]:
    """
    Read time series related to external time series ID specified in manual mapping input

    Args:
        client: Instance of CogniteClient
        manual_mappings: list of manual mappings

    Returns:
        list of TimeSeries
    """
    try:
        ts_xid_list = [mapping[COL_KEY_MAN_MAPPING_TS_EXTID] for mapping in manual_mappings]
        ts_list_manual_mapping = client.time_series.retrieve_multiple(external_ids=ts_xid_list)
    except Exception as e:
        print(f"ERROR: Not able read list of time series from {manual_mappings} - error: {e}")
        return []

    if len(ts_list_manual_mapping) == len(ts_xid_list):
        return ts_list_manual_mapping

    raise ValueError(
        f"Number of time series found {len(ts_list_manual_mapping)} does not match number of "
        f"time series in input list {len(ts_xid_list)}"
    )


def get_time_series(
    client: CogniteClient, config: ContextConfig, manual_matches: list[Row]
) -> tuple[list[dict[str, Any]], list[TimeSeries]]:
    """
    Read time series based on root ASSET id
    Read all if config property readAll = True, else only read time series not contextualized ( connected to asset)

    Args:
        client: Instance of CogniteClient
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
        for ts_prefix in config.time_series_prefix:
            ts_list = client.time_series.list(
                data_set_external_ids=[config.time_series_data_set_ext_id],
                external_id_prefix=ts_prefix,
                limit=None,
            )
            # get list of time series in manual mappings to exclude from matching
            to_skip = set(m_match[COL_KEY_MAN_MAPPING_TS_EXTID] for m_match in manual_matches)

            for ts in ts_list:
                # if manual match exists, skip matching based on entity matching
                if ts.external_id in to_skip or ts.external_id is None:
                    continue

                if TS_CONTEXTUALIZED_METADATA_KEY not in ts.metadata or {}:
                    entities = get_ts_entities(ts, entities)
                    ts_meta_dict[ts.id] = ts.metadata

                # if run all - remove metadata element from last annotation
                elif config.run_all:
                    if not config.debug:
                        meta_ts_update.append(
                            TimeSeriesUpdate(external_id=ts.external_id).metadata.remove(
                                [TS_CONTEXTUALIZED_METADATA_KEY]
                            )
                        )
                    entities = get_ts_entities(ts, entities)
                    ts_meta_dict[ts.id] = ts.metadata

        client.time_series.update(meta_ts_update)
        return entities, ts_meta_dict

    except Exception as e:
        print(
            f"ERROR: Not able to get entities for time series in data set: {config.time_series_data_set_ext_id} "
            f"with prefix: {config.time_series_prefix}- error: {e}"
        )
        return [], {}


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
        print(f"WARNING: No name found for time series with external ID: {ts.external_id}, and metadata: {ts}")
        return entities

    # build list with possible file name variations used in P&ID to refer to other P&ID
    split_name = re.split("[,._ \\-!?:]+", ts.name)
    mod_name = ts.name[len(split_name[0]) + 1 :]  # remove prefix
    mod_name = mod_name[: -(len(split_name[-1]) + 1)]  # remove postfix

    # add entities for files used to match between file references in P&ID to other files
    entities.append({"id": ts.id, "name": mod_name, "external_id": ts.external_id, "org_name": ts.name, "type": "ts"})
    return entities


def tag_is_dummy(asset: Asset) -> bool:
    custom_description = (asset.metadata or {}).get("Description", "")
    return "DUMMY TAG" in custom_description.upper()


def get_assets(client: CogniteClient, asset_root_ext_id: str, read_limit: int) -> list[dict[str, Any]]:
    """
    Get Asset used as input to contextualization and build list of entities

    Args:
        client: Instance of CogniteClient
        asset_root_ext_id: external root asset ID
        read_limit : number of assets to read

    Returns:
        list of entities
    """
    entities: list[dict[str, Any]] = []
    try:
        assets = client.assets.list(asset_subtree_external_ids=[asset_root_ext_id], limit=read_limit)
        for asset in assets:
            if tag_is_dummy(asset):
                continue

            # Do any manual updates changes to name to clean up and make it easier to match
            name = asset.name
            split_name = re.split("[,._ \\-:]+", name)
            if len(name) > 3 and len(split_name) >= 3:
                entities.append(
                    {
                        "id": asset.id,
                        "name": name,
                        "external_id": asset.external_id,
                        "org_name": asset.name,
                        "type": "asset",
                    }
                )
    except Exception as e:
        print(f"ERROR: Not able to get entities for asset extId root: {asset_root_ext_id}. Error: {type(e)}({e})")
    return entities


def get_matches(
    client: CogniteClient, match_to: list[dict[str, Any]], match_from: list[dict[str, Any]]
) -> list[ContextualizationJob]:
    """
    Create / Update entity matching model and run job to get matches

    Args:
        client: Instance of CogniteClient
        match_to: list of entities to match to (target)
        match_from: list of entities to match from (source)

    Returns:
        list of matches
    """
    try:
        model = client.entity_matching.fit(
            sources=match_from,
            targets=match_to,
            match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
            feature_type=ML_MODEL_FEATURE_TYPE,
        )
        job = model.predict(sources=match_from, targets=match_to, num_matches=1)
        return job.result["items"]

    except Exception as e:
        print(f"ERROR: Failed to get matching model and run prediction. Error: {type(e)}({e})")
        raise
