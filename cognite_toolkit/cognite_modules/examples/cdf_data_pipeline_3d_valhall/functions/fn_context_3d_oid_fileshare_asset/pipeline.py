from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun, Row, ThreeDAssetMapping
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_3D_NODE_NAME,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
)
from get_resources import (
    filter_3d_nodes,
    get_3d_model_id_and_revision_id,
    get_3d_nodes,
    get_asset_id_ext_id_mapping,
    get_assets,
    get_mapping_to_delete,
    get_matches,
    get_treed_asset_mappings,
    read_manual_mappings,
)
from write_resources import write_mapping_to_raw


def annotate_3d_model(client: CogniteClient, config: ContextConfig) -> None:
    """
    Read configuration and start process by
    1. Read RAW table with manual mappings and extract all rows not contextualized
    2. Apply manual mappings from 3D nodes to Asset - this will overwrite any existing mapping
    3. Read all time series not matched (or all if runAll is True)
    4. Read all assets
    5. Run ML contextualization to match 3D Nodes -> Assets
    6. Update 3D Nodes with mapping
    7. Write results matched (good) not matched (bad) to RAW
    8. Output in good/bad table can then be used in workflow to update manual mappings

    Args:
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """
    print("INFO: Initiating 3D annotation process")

    len_good_matches = 0
    len_bad_matches = 0
    old_mappings_removed = 0
    manual_mappings = []
    existing_matches = {}
    mapping_to_delete = None
    numAsset = -1 if not config.debug else 10000

    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    try:
        # get model id and revision id based on name
        model_id, revision_id = get_3d_model_id_and_revision_id(client, config, config.three_d_model_name)

        manual_mappings, existing_matches = read_manual_mappings(client, config)

        if config.run_all or not config.keep_old_mapping:
            mapping_to_delete = get_mapping_to_delete(client, model_id, revision_id)

        if len(manual_mappings) > 0:
            tree_d_nodes = filter_3d_nodes(client, config, model_id, revision_id, manual_mappings)

            # Start by applying manual mappings - NOTE manual mappings will write over existing mappings
            existing_matches = apply_manual_mappings(
                client, config, raw_uploader, manual_mappings, existing_matches, model_id, revision_id, tree_d_nodes
            )

        # get existing Asset matches
        if config.keep_old_mapping and not config.run_all:
            existing_matches = get_treed_asset_mappings(client, model_id, revision_id, existing_matches)

        asset_entities = get_assets(client, config.asset_root_ext_id, existing_matches, numAsset)
        if not asset_entities:
            raise Exception("WARNING: No assets found for root asset: {config.asset_root_ext_id}")

        three_d_entities, tree_d_nodes = get_3d_nodes(client, config, asset_entities, model_id, revision_id)

        # If there is any 3D nodes to be contextualized
        if len(three_d_entities) > 0:
            match_results = get_matches(client, asset_entities, three_d_entities)

            good_matches, bad_matches, existing_matches = select_and_apply_matches(
                client, config, match_results, tree_d_nodes, model_id, revision_id, existing_matches
            )

            write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches)
            len_good_matches = len(good_matches)
            len_bad_matches = len(bad_matches)

            if config.run_all or not config.keep_old_mapping and len(mapping_to_delete) > 0:
                old_mappings_removed = remove_old_mappings(
                    client, mapping_to_delete, existing_matches, model_id, revision_id
                )

        msg = (
            f"Contextualization of 3D to asset root: {config.asset_root_ext_id}, num manual mappings: "
            f"{len(manual_mappings)}, num 3D nodes contextualized: {len_good_matches}, num 3D nodes NOT contextualized : "
            f"{len_bad_matches} (score below {config.match_threshold}) "
            f"and: {old_mappings_removed} old mappings removed"
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
        msg = f"Contextualization of time series for root asset: {config.asset_root_ext_id} failed - Message: {e!s}"
        print(f"ERROR: {msg}")
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(
                extpipe_external_id=config.extraction_pipeline_ext_id,
                status="failure",
                message=shorten(msg, 1000),
            )
        )


def apply_manual_mappings(
    client: CogniteClient,
    config: ContextConfig,
    raw_uploader: RawUploadQueue,
    manual_mappings: list[Row],
    existing_matches: dict[str, Any],
    model_id: int,
    revision_id: int,
    tree_d_nodes: dict[str, Any],
) -> dict[str, Any]:
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
    mapping = {}

    # IF run_all then all manual matches should also be updated
    if config.run_all:
        existing_matches = {}

    try:
        three_d_node_asset_id = get_asset_id_ext_id_mapping(manual_mappings)

        asset_mappings = []
        for node, node_values in tree_d_nodes.items():
            asset_id = three_d_node_asset_id[node][0]
            for node_value in node_values:
                if asset_id in existing_matches:
                    existing_matches[asset_id].append(node_value["id"])
                else:
                    existing_matches[asset_id] = [node_value["id"]]

                asset_mappings.append(
                    ThreeDAssetMapping(
                        node_id=node_value["id"],
                        asset_id=asset_id,
                    )
                )

            mapping[COL_KEY_MAN_MAPPING_3D_NODE_NAME] = node
            mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] = three_d_node_asset_id[node][1]
            mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_manual, Row(node, mapping))

            if len(asset_mappings) > 0 and len(asset_mappings) % 10000 == 0:
                if not config.debug:
                    client.three_d.asset_mappings.create(
                        model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
                    )
                    print(f"Updated {len(asset_mappings)} 3D mappings")
                    asset_mappings = []

        if not config.debug:
            client.three_d.asset_mappings.create(
                model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
            )
            print(f"Updated {len(asset_mappings)} nodes with 3D mappings")

        raw_uploader.upload()
        return existing_matches

    except Exception as e:
        print(f"ERROR: Applying manual mappings. Error: {type(e)}({e})")


def select_and_apply_matches(
    client: CogniteClient,
    config: ContextConfig,
    match_results: list[ContextualizationJob],
    tree_d_nodes: dict[str, Any],
    model_id: int,
    revision_id: int,
    existing_matches: dict[str, Any],
) -> tuple[list[Row], list[Row], dict[str, Any]]:
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
    mapped_node = []
    asset_mappings = []

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

        for match in good_matches:
            node_str = match["3DNameMatched"]

            if node_str not in mapped_node:
                mapped_node.append(node_str)

                asset_id = match["assetId"]
                node_ids = tree_d_nodes[node_str]

                for node_id in node_ids:
                    if asset_id in existing_matches:
                        existing_matches[asset_id].append(node_id["id"])
                    else:
                        existing_matches[asset_id] = [node_id["id"]]

                    asset_mappings.append(
                        ThreeDAssetMapping(
                            node_id=node_id["id"],
                            asset_id=asset_id,
                        )
                    )

                if len(asset_mappings) > 0 and len(asset_mappings) % 10000 == 0:
                    if not config.debug:
                        client.three_d.asset_mappings.create(
                            model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
                        )
                        print(f"Updated {len(asset_mappings)} 3D mappings")
                        asset_mappings = []

        if not config.debug:
            client.three_d.asset_mappings.create(
                model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
            )
            print(f"INFO: Updated {len(asset_mappings)} nodes with 3D mappings")

        return good_matches, bad_matches, existing_matches

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

    try:
        mFrom = match["source"]

        if len(match["matches"]) > 0:
            mTo = match["matches"][0]["target"]
            score = match["matches"][0]["score"]
            asset_name = mTo["name"]
            asset_id = mTo["id"]
        else:
            score = 0
            asset_name = "_no_match_"
            asset_id = None

        return {
            "score": score,
            "3DName": mFrom["org_name"],
            "3DNameMatched": mFrom["name"],
            "3DId": mFrom["id"],
            "assetName": asset_name,
            "assetId": asset_id,
        }
    except Exception as e:
        raise Exception(f"ERROR: Not able to parse return object: {match} - error: {e}")


def remove_old_mappings(
    client: CogniteClient,
    mapping_to_delete: list[ThreeDAssetMapping],
    existing_matches: dict[str, Any],
    model_id: int,
    revision_id: int,
) -> int:
    delete_mapping = []

    for mapping in mapping_to_delete:
        asset_id = mapping.asset_id

        if asset_id not in existing_matches:
            delete_mapping.append(mapping)

    if len(delete_mapping) > 0:
        client.three_d.asset_mappings.delete(model_id=model_id, revision_id=revision_id, asset_mapping=delete_mapping)

    print(f"INFO: Deleted {len(delete_mapping)} old mappings")

    return len(delete_mapping)
