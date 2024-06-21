from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, ContextualizationJob, Row
from cognite.client.data_classes.three_d import ThreeDAssetMapping

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_3D_NODE_NAME,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_ASSET_ID,
    COL_MATCH_KEY,
    MAX_MODEL_SIZE_TO_CREATE_MODEL,
    ML_MODEL_FEATURE_TYPE,
)


def manual_table_exists(client: CogniteClient, config: str) -> bool:
    tables = client.raw.tables.list(config.rawdb, limit=None)
    return any(tbl.name == config.raw_table_manual for tbl in tables)


def read_manual_mappings(client: CogniteClient, config: ContextConfig) -> tuple[list[Row], dict[str, Any]]:
    """
    Read manual mappings from RAW DB and add to list of manual mappings if not already contextualized

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig

    Returns:
        list of manual mappings or empty list if no mappings are found
    """
    existing_matches = {}
    manual_mappings = []
    seen_mappings = set()
    try:
        if not manual_table_exists(client, config):
            return manual_mappings

        row_list = client.raw.rows.list(config.rawdb, config.raw_table_manual, limit=-1)

        if len(row_list) > 0:
            asset_ext_id_list = [mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID] for mapping in row_list]
            asset_list = client.assets.retrieve_multiple(external_ids=list(set(asset_ext_id_list)))
            asset_ext_id_dict = {asset.external_id: asset.id for asset in asset_list}

        for row in row_list:
            asset_id = asset_ext_id_dict[row.columns[COL_KEY_MAN_MAPPING_ASSET_EXTID].strip()]
            existing_matches[asset_id] = [row.columns[COL_KEY_MAN_MAPPING_3D_NODE_NAME].strip()]
            if not (
                config.run_all
                or COL_KEY_MAN_CONTEXTUALIZED not in row.columns
                or row.columns[COL_KEY_MAN_CONTEXTUALIZED] is not True
            ):
                continue

            # Make sure we don't add duplicate TS external IDs
            three_d_node_name = row.columns[COL_KEY_MAN_MAPPING_3D_NODE_NAME].strip()
            if three_d_node_name not in seen_mappings:
                seen_mappings.add(three_d_node_name)
                manual_mappings.append(
                    {
                        COL_KEY_MAN_MAPPING_3D_NODE_NAME: three_d_node_name,
                        COL_KEY_MAN_MAPPING_ASSET_EXTID: row.columns[COL_KEY_MAN_MAPPING_ASSET_EXTID].strip(),
                        COL_KEY_MAN_MAPPING_ASSET_ID: asset_id,
                    }
                )

        print(f"INFO: Number of manual mappings: {len(manual_mappings)}")
        return manual_mappings, existing_matches

    except Exception as e:
        raise Exception(f"ERROR: Read manual mappings. Error: {type(e)}({e})")


def get_asset_id_ext_id_mapping(manual_mappings: list[Row]) -> dict[str, Any]:
    """
    Read assets specified in manual mapping input based on external ID and find the corresponding asset internal ID
    Internal ID is used to update time series with asset ID

    Args:
        manual_mappings: list of manual mappings

    Returns:
        dictionary with asset external id as key and asset id as value

    """
    try:
        three_d_node_asset_id = {}
        for mapping in manual_mappings:
            three_d_node_asset_id[mapping[COL_KEY_MAN_MAPPING_3D_NODE_NAME]] = [
                mapping[COL_KEY_MAN_MAPPING_ASSET_ID],
                mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID],
            ]

        return three_d_node_asset_id

    except Exception as e:
        raise Exception(f"ERROR: Not able read list of assets from {manual_mappings}. Error: {type(e)}({e})")


def get_3d_model_id_and_revision_id(
    client: CogniteClient, config: ContextConfig, three_d_model_name: str
) -> tuple[int, int]:
    try:
        model_id_list = [
            model.id
            for model in client.three_d.models.list(published=True, limit=1)
            if model.name == three_d_model_name
        ]
        if not model_id_list:
            raise ValueError(f"3D model with name {three_d_model_name} not found")
        model_id = model_id_list[0]

        revision_list = client.three_d.revisions.list(model_id=model_id, published=True)
        if not revision_list:
            raise ValueError(f"3D model with name {three_d_model_name} has no published revisions")
        revision = revision_list[0]  # get latest revision

        print(f"INFO: For Model name: {three_d_model_name} using 3D model ID: {model_id} - revision ID: {revision.id}")
        print("If wrong model ID/revision remove other published versions of the model and try again")

        return model_id, revision.id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for 3D nodes in data set: {config.three_d_data_set_ext_id}- error: {e}"
        )


def get_mapping_to_delete(client: CogniteClient, model_id: int, revision_id: int) -> list[ThreeDAssetMapping]:
    mapping_to_delete = client.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id)

    return mapping_to_delete


def get_treed_asset_mappings(
    client: CogniteClient, model_id: int, revision_id: int, existing_matches: dict[str, Any]
) -> dict[str, Any]:
    mappings = client.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id, limit=-1)

    for mapping in mappings.data:
        if mapping.asset_id in existing_matches:
            existing_matches[mapping.asset_id].append(mapping.node_id)
        else:
            existing_matches[mapping.asset_id] = [mapping.node_id]

    return existing_matches


def filter_3d_nodes(
    client: CogniteClient,
    config: ContextConfig,
    model_id: int,
    revision_id: int,
    manual_mappings: list[Row],
) -> dict[str, Any]:
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
    tree_d_nodes = {}

    node_names = [manual_mappings["3dNodeName"] for manual_mappings in manual_mappings]
    try:
        # read 3D nodes from API with filter on node names
        three_d_nodes = client.three_d.revisions.filter_nodes(
            model_id=model_id,
            revision_id=revision_id,
            properties={"Item": {"Name": node_names}},
            partitions=10,
            limit=-1,
        )

        num_nodes = 0
        for node in three_d_nodes:
            if node.name and node.name != "":
                num_nodes += 1

                if node.name in tree_d_nodes:
                    node_ids = tree_d_nodes[node.name]
                    node_ids.append(
                        {
                            "id": node.id,
                            "subtree_size": node.subtree_size,
                            "tree_index": node.tree_index,
                        }
                    )
                else:
                    node_ids = [
                        {
                            "id": node.id,
                            "subtree_size": node.subtree_size,
                            "tree_index": node.tree_index,
                        }
                    ]
                    tree_d_nodes[node.name] = node_ids

        print(
            f"INFO: Total number of 3D Node names found for manual mapping: {num_nodes} - unique names : {len(tree_d_nodes)}"
        )

        return tree_d_nodes

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for 3D nodes in data set: {config.three_d_data_set_ext_id}- error: {e}"
        )


def get_3d_nodes(
    client: CogniteClient,
    config: ContextConfig,
    asset_entities: list[dict[str, Any]],
    model_id: int,
    revision_id: int,
    numNodes: int = -1,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
    entities: list[dict[str, Any]] = []
    cdf_3d_nodes = []
    three_d_nodes = {}
    input_three_d_nodes = None

    three_d_model_name = config.three_d_model_name
    try:
        # prep list of asset filters
        asset_filter = [asset["name"] for asset in asset_entities]
        three_d_data_set_id = client.data_sets.retrieve(external_id=config.three_d_data_set_ext_id).id

        model_file_name = f"3D_nodes_{three_d_model_name}_id_{model_id}_rev_id_{revision_id}.json"
        if not config.run_all:
            three_d_file = client.files.retrieve(external_id=model_file_name)
            if three_d_file:
                file_content = client.files.download_bytes(external_id=model_file_name)
                input_three_d_nodes = json.loads(file_content)
        if not input_three_d_nodes:
            # read 3D nodes from API
            # Filter used for debugging and testing
            # input_three_d_nodes_cdf = client.three_d.revisions.filter_nodes(
            #    model_id=model_id, revision_id=revision_id, properties={"Item": {"Name": ["/23-VA-9110-M01/E1"]}}, partitions=10, limit=-1
            # )

            input_three_d_nodes_cdf = client.three_d.revisions.list_nodes(
                model_id=model_id, revision_id=revision_id, sort_by_node_id=True, partitions=100, limit=-1
            )

            for node in input_three_d_nodes_cdf:
                if node.name and node.name != "":
                    mod_node_name = node.name
                    if "/" in mod_node_name:
                        mod_node_name = mod_node_name.split("/", 2)[1]

                    if mod_node_name in asset_filter:
                        cdf_3d_nodes.append(node.dump())

            file_content = json.dumps(cdf_3d_nodes)
            client.files.upload_bytes(
                file_content,
                external_id=model_file_name,
                name=model_file_name,
                overwrite=True,
                data_set_id=three_d_data_set_id,
            )

            input_three_d_nodes = json.loads(file_content)

        num_nodes = 0
        if input_three_d_nodes:
            for node in input_three_d_nodes:
                if node["name"] and node["name"] != "":
                    num_nodes += 1
                    mod_node_name = node["name"]
                    if "/" in mod_node_name:
                        mod_node_name = mod_node_name.split("/", 2)[1]

                    if mod_node_name in asset_filter:
                        if mod_node_name in three_d_nodes:
                            node_ids = three_d_nodes[mod_node_name]
                            node_ids.append(
                                {
                                    "id": node["id"],
                                    "subtree_size": node["subtreeSize"],
                                    "tree_index": node["treeIndex"],
                                }
                            )
                        else:
                            node_ids = [
                                {
                                    "id": node["id"],
                                    "subtree_size": node["subtreeSize"],
                                    "tree_index": node["treeIndex"],
                                }
                            ]
                            three_d_nodes[mod_node_name] = node_ids

                            entities = get_3d_entities(node, mod_node_name, entities)

        print(
            f"INFO: Total number of 3D Node found: {num_nodes} - unique names to match after asset name filtering: {len(three_d_nodes)}"
        )

        return entities, three_d_nodes

    except Exception as e:
        raise Exception(f"ERROR: Not able to get 3D nodes in data set: {config.three_d_data_set_ext_id} - error: {e}")


def get_3d_entities(node: list[str], modNodeName: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    process time series metadata and create an entity used as input to contextualization

    Args:
        node: metadata for 3D node
        modNodeName: modified node name
        entities: already processed entities

    Returns:
        list of entities
    """

    # add entities for files used to match between 3D nodes and assets
    entities.append(
        {
            "id": node["id"],
            "name": modNodeName,
            "external_id": node["treeIndex"],
            "org_name": node["name"],
            "type": "3dNode",
        }
    )
    return entities


def tag_is_dummy(asset: Asset) -> bool:
    custom_description = (asset.metadata or {}).get("Description", "")
    return "DUMMY TAG" in custom_description.upper()


def get_assets(
    client: CogniteClient, asset_root_ext_id: str, existing_matches: list[dict[str, Any]], read_limit: int
) -> list[dict[str, Any]]:
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

            # Skip if asset is already matched
            if asset.id in existing_matches:
                continue

            # Do any manual updates changes to name t  o clean up and make it easier to match
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
        print(f"INFO: Number of assets found: {len(entities)}")

        return entities

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for asset extId root: {asset_root_ext_id}. Error: {type(e)}({e})"
        )


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

    more_to_match = True
    all_matches = []
    match_size = MAX_MODEL_SIZE_TO_CREATE_MODEL
    min_match_size = int(MAX_MODEL_SIZE_TO_CREATE_MODEL / 4)
    offset = 0
    retry_num = 3
    match_array = []

    try:
        # limit number input nodes to create model
        if len(match_from) > MAX_MODEL_SIZE_TO_CREATE_MODEL:
            sources = match_from[:MAX_MODEL_SIZE_TO_CREATE_MODEL]
        else:
            sources = match_from

        if len(match_to) > MAX_MODEL_SIZE_TO_CREATE_MODEL:
            targets = match_to[:MAX_MODEL_SIZE_TO_CREATE_MODEL]
        else:
            targets = match_to

        model = client.entity_matching.fit(
            sources=sources,
            targets=targets,
            match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
            feature_type=ML_MODEL_FEATURE_TYPE,
        )

        while more_to_match:
            if len(match_from) < offset + match_size:
                more_to_match = False
                match_array = match_from[offset:]
            else:
                match_array = match_from[offset : offset + match_size]

            print(f"INFO: Run mapping of number of nodes from: {offset} to {offset + len(match_array)}")

            try:
                job = model.predict(sources=match_array, targets=targets, num_matches=1)
                matches = job.result
                all_matches = all_matches + matches["items"]
                offset += match_size
                retry_num = 3  # reset retry
            except Exception as e:
                retry_num -= 1
                if retry_num < 0:
                    raise Exception("Not able not run mapping job, giving up after retry - error: {e}") from e
                else:
                    more_to_match = True
                    if int(match_size / 2) > min_match_size:
                        match_size = int(match_size / 2)
                    print(f"ERROR: Not able not run mapping job  - error: {e}")
                    pass

        return all_matches

    except Exception as e:
        raise Exception(f"ERROR: Failed to get matching model and run fit / matching. Error: {type(e)}({e})")
