from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, Row, TimeSeries, TimeSeriesUpdate

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_TS_EXTID,
    COL_MATCH_KEY,
    TS_CONTEXTUALIZED_METADATA_KEY,
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

            print(f"INFO: Number of manual mappings: {len(manual_mappings)}")

    except Exception as e:
        print(f"ERROR: Read manual mappings. Error: {e}")

    return manual_mappings


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
        print(f"ERROR: Not able read list of assets from {manual_mappings} - error: {e}")

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
                f"Number of time series found {len(ts_list_manual_mapping)} does not match number of "
                f"time series in input list {len(ts_ext_id_list)}"
            )

    except Exception as e:
        print(f"ERROR: Not able read list of time series from {manual_mappings} - error: {e}")

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
        for ts_prefix in config.time_series_prefix:
            ts_list = cognite_client.time_series.list(
                data_set_external_ids=[config.time_series_data_set_ext_id], external_id_prefix=ts_prefix
            )
            # get list of time series in manual mappings to exclude from matching
            ts_ext_id_manual_match_list = [m_match[COL_KEY_MAN_MAPPING_TS_EXTID] for m_match in manual_matches]

            for ts in ts_list:
                # if manual match exists, skip matching based on entity matching
                if ts.external_id in ts_ext_id_manual_match_list:
                    continue

                if TS_CONTEXTUALIZED_METADATA_KEY is not None and TS_CONTEXTUALIZED_METADATA_KEY not in (
                    ts.metadata or {}
                ):
                    if ts.external_id is not None:
                        entities = get_ts_entities(ts, entities)
                        ts_meta_dict[ts.id] = ts.metadata

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

        if len(meta_ts_update) > 0:
            cognite_client.time_series.update(meta_ts_update)

    except Exception as e:
        print(
            f"ERROR: Not able to get entities for time series in data set: {config.time_series_data_set_ext_id} "
            f"with prefix: {config.time_series_prefix}- error: {e}"
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
        print(f"WARNING: No name found for time series with external ID: {ts.external_id}, and metadata: {ts}")
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
        print(f"ERROR: Not able to get entities for asset extId root: {asset_root_ext_id} - error: {e}")

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
        print("INFO: Run prediction based on model")
        job = model.predict(sources=match_from, targets=match_to, num_matches=1)
        matches = job.result
        return matches["items"]

    except Exception as e:
        print(f"ERROR: Failed to get matching model and run prediction - error: {e}")
