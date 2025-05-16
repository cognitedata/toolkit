from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

import pytz  # type: ignore
from cognite.client import CogniteClient
from common.dataclass.common import Vec3f

logger = logging.getLogger(__name__)


def create_apm_observation_from_reading(
    client: CogniteClient,
    file_external_id: str,
    timeseries_external_id: str,
    value: float,
    timestamp: int,
    pose: Vec3f,
    message: str,
    apm_checklistitem_external_id: Optional[str] = None,
):
    """Create an APM observation from a gauge reading.

    NOTE: this is a preliminary implementation, where we're not yet using the
    gql pygen SDK made by Cognite (which will make this a lot cleaner)

    """
    apm_config = _get_apm_config(client=client, config_external_id="inrobot")
    if apm_config is None:
        logger.error("Could not find APM config")
        return

    space_id = str(apm_config.get("appDataSpaceId"))
    appdata_space_version = str(apm_config.get("appDataSpaceVersion"))

    views = _get_views(
        client=client,
        space=space_id,
        datamodel_id=space_id,
        datamodel_version=appdata_space_version,
        view_ids=["APM_Measurement", "Vec3f", "APM_Observation"],
    )

    def _get_view_version(views: list[dict[str, Any]], view_id: str) -> Optional[str]:
        view = next((item for item in views if item["externalId"] == view_id), None)
        if view is None:
            raise ValueError(f"Could not find view {view_id}")
        return view.get("version")

    # create the APM measurement
    if isinstance(timestamp, int):
        timestamp_s = round(timestamp / 1000)
        timestamp_dt = datetime.fromtimestamp(float(timestamp_s), pytz.UTC)
    elif isinstance(timestamp, datetime):
        timestamp_dt = timestamp
    else:
        raise ValueError(f"Timestamp is of type {type(timestamp)}, needs to be either `int` or `datetime`.")

    apm_measurement_external_id = f"measurement_{timeseries_external_id}_{timestamp}"
    apm_measurement = {
        "timeSeries": timeseries_external_id,
        "measuredAt": timestamp_dt.isoformat(),
        "numericValue": value,
    }

    vec3f_external_id = f"position_{timeseries_external_id}_{timestamp}"
    vec3f = {"x": pose.x, "y": pose.y, "z": pose.z}

    apm_observation_external_id = f"observation_{timeseries_external_id}_{timestamp}"
    apm_observation = {
        "fileIds": [file_external_id],
        "position": {"externalId": vec3f_external_id, "space": space_id},
        "description": "Gauge reading",
        "note": message,
        "createdById": "CDF Gauge Reader Function",
        "updatedById": "CDF Gauge Reader Function",
    }

    # edge apm_observation -> apm_measurement
    edge_apm_observation_apm_measurement = {
        "instanceType": "edge",
        "space": space_id,
        "externalId": f"{apm_observation_external_id}.measurements_{apm_measurement_external_id}",
        "startNode": {"externalId": apm_observation_external_id, "space": space_id},
        "endNode": {"externalId": apm_measurement_external_id, "space": space_id},
        "type": {"externalId": "APM_Observation.measurements", "space": space_id},
    }

    # assemble all instances (nodes and edges)
    instances = [
        {
            "instanceType": "node",
            "space": space_id,
            "externalId": apm_measurement_external_id,
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": space_id,
                        "externalId": "APM_Measurement",
                        "version": _get_view_version(views, "APM_Measurement"),
                    },
                    "properties": apm_measurement,
                }
            ],
        },
        {
            "instanceType": "node",
            "space": space_id,
            "externalId": vec3f_external_id,
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": space_id,
                        "externalId": "Vec3f",
                        "version": _get_view_version(views, "Vec3f"),
                    },
                    "properties": vec3f,
                }
            ],
        },
        {
            "instanceType": "node",
            "space": space_id,
            "externalId": apm_observation_external_id,
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": space_id,
                        "externalId": "APM_Observation",
                        "version": _get_view_version(views, "APM_Observation"),
                    },
                    "properties": apm_observation,
                }
            ],
        },
        edge_apm_observation_apm_measurement,
    ]

    if apm_checklistitem_external_id is not None:
        edge_apm_checklist_apm_observation = {
            "instanceType": "edge",
            "space": space_id,
            "externalId": f"{apm_checklistitem_external_id}.observations_{apm_observation_external_id}",
            "startNode": {"externalId": apm_checklistitem_external_id, "space": space_id},
            "endNode": {"externalId": apm_observation_external_id, "space": space_id},
            "type": {"externalId": "APM_ChecklistItem.observations", "space": space_id},
        }
        instances.append(edge_apm_checklist_apm_observation)

    logger.debug(f"Creating instances request: {json.dumps(instances, indent=2)}")

    response = client.post(
        f"/api/v1/projects/{client.config.project}/models/instances", json={"items": instances}
    ).json()

    logger.debug(f"Creating instances response: {json.dumps(response, indent=2)}")


def _get_views(
    client: CogniteClient, space: str, datamodel_id: str, datamodel_version: Optional[str] = None, view_ids=list[str]
) -> list[dict[str, Any]]:
    response = client.post(
        f"/api/v1/projects/{client.config.project}/models/datamodels/byids",
        json={
            "items": [{"space": space, "externalId": datamodel_id, "version": datamodel_version}],
        },
    ).json()

    # get latest datamodel, if no version is specified
    datamodel = max(response["items"], key=lambda item: item["createdTime"], default=None)
    if datamodel is None:
        raise ValueError(f"No datamodels with id {datamodel_id} found with version {datamodel_version}.")

    views = datamodel.get("views")
    if views is None or views == []:
        raise ValueError(f"Datamodel {datamodel_id} has no views.")

    available_views = {view.get("externalId"): view.get("version") for view in views}

    items = [
        {"space": space, "externalId": requested_view_id, "version": available_views.get(requested_view_id)}
        for requested_view_id in view_ids
        if available_views.get(requested_view_id) is not None
    ]

    if len(items) != len(view_ids):
        missing_views = set(view_ids) - set([item["externalId"] for item in items])
        raise ValueError(f"Views {', '.join(missing_views)} not found in datamodel {datamodel_id}.")

    views = client.post(
        f"/api/v1/projects/{client.config.project}/models/views/byids",
        json={
            "items": items,
        },
    ).json()

    return views["items"]


def _get_apm_config(client: CogniteClient, config_external_id: str) -> Optional[dict[str, Any]]:
    space = "APM_Config"
    datamodel_id = "APM_Config"
    view_id = "APM_Config"
    instance_external_ids = [config_external_id, "default-config"]

    views = _get_views(client=client, space=space, datamodel_id=datamodel_id, view_ids=[view_id])

    if len(views) != 1:
        raise ValueError(f"Expected to find exactly one view for {view_id}, found {len(views)}.")

    view = views[0]

    response = client.post(
        f"/api/v1/projects/{client.config.project}/models/instances/byids",
        json={
            "items": [
                {"instanceType": "node", "externalId": external_id, "space": space}
                for external_id in instance_external_ids
            ],
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": space,
                        "externalId": view.get("externalId"),
                        "version": view.get("version"),
                    }
                }
            ],
        },
    ).json()

    if response.get("items") == []:
        logger.error("Could not find APM config, not upserting any APM_Observations now.")
        return None

    # Try to find the first item with "externalId" == config_external_id
    apm_config = next((item for item in response["items"] if item["externalId"] == config_external_id), None)

    # If no such item is found, try to find the first item with "externalId" == "default"
    if apm_config is None:
        apm_config = next((item for item in response["items"] if item["externalId"] == "default-config"), None)
        if apm_config is None:
            raise ValueError(f"Could not find APM config with externalId {config_external_id} or `default-config`.")

    apm_config = apm_config.get("properties").get(space).get(f"{view.get('externalId')}/{view.get('version')}")

    return apm_config


def _get_position_from_metadata_to_vec3f(metadata: dict[str, Any]) -> Vec3f:
    """Get position from metadata."""
    x = metadata.get("waypoint_tform_body_x")
    y = metadata.get("waypoint_tform_body_x")
    z = metadata.get("waypoint_tform_body_x")

    if not x or not y or not z:
        raise ValueError(
            f"Missing metadata field. Required metadata fields \
            are waypoint_tform_body_x, waypoint_tform_body_x, waypoint_tform_body_x. File metadata keys: {metadata.keys()}"
        )

    return Vec3f(x=float(x), y=float(y), z=float(z))
