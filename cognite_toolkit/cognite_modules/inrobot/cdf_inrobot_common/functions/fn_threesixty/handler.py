"""Gauge reader handler."""

import io
import json
import logging
import sys
from typing import Any, Optional

import numpy as np
from bosdyn.client.math_helpers import Quat, SE3Pose
from cognite.client import CogniteClient, global_config
from cognite.client.data_classes import FileMetadata, FileMetadataList, FileMetadataUpdate, LabelFilter
from cognite.client.exceptions import CogniteDuplicatedError
from cognite_threesixty_images import CogniteThreeSixtyImageExtractor, VectorXYZ
from PIL import Image
from scipy.spatial.transform import Rotation

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=logging.BASIC_FORMAT)
logger = logging.getLogger(__name__)
CUBEMAP_RESOLUTION = 1024
global_config.disable_gzip = True


def get_map_transform_from_map(client: CogniteClient, map_external_id: str) -> Optional[dict]:
    """Get the transform of a map in the current project based on mission. We asume the frame has the root frame as parent."""
    project = client._config.project
    map_request = {"items": [{"externalId": map_external_id}]}

    res = client.post(url=f"/api/v1/projects/{project}/robotics/maps/byids", json=map_request).json()
    for map in res["items"]:
        frame_external_id = map.get("frameExternalId")
    assert frame_external_id, f"Map {map_external_id} does not have a frame associated with it."
    # Create mapping
    frame_request = {"items": [{"externalId": frame_external_id}]}

    res = client.post(
        url=f"/api/v1/projects/{project}/robotics/frames/byids",
        json=frame_request,
    ).json()

    print(f"going to get frame {frame_external_id} from map {map_external_id}")

    for item in res["items"]:
        if item.get("transform"):
            print(f"found translation {item.get('transform').get('orientation').get('w')}")
            return SE3Pose(
                x=item.get("transform").get("translation").get("x"),
                y=item.get("transform").get("translation").get("y"),
                z=item.get("transform").get("translation").get("z"),
                rot=Quat(
                    w=item.get("transform").get("orientation").get("w"),
                    x=item.get("transform").get("orientation").get("x"),
                    y=item.get("transform").get("orientation").get("y"),
                    z=item.get("transform").get("orientation").get("z"),
                ),
            )
    return None


def convert_metadata_to_se3_pose(external_id: str, metadata: dict[str, Any]) -> Optional[SE3Pose]:
    """Convert metadata to SE3Pose."""
    x = metadata.get("waypoint_tform_body_x")
    y = metadata.get("waypoint_tform_body_x")
    z = metadata.get("waypoint_tform_body_x")
    qx = metadata.get("waypoint_tform_body_qx")
    qy = metadata.get("waypoint_tform_body_qy")
    qz = metadata.get("waypoint_tform_body_qz")
    qw = metadata.get("waypoint_tform_body_qw")
    if not x or not y or not z or not qx or not qy or not qz or not qw:
        logger.error(
            f"Failed to process file {external_id}. Missing metadata field. Required metadata fields \
            are waypoint_tform_body_x, waypoint_tform_body_x, waypoint_tform_body_x, waypoint_tform_body_qx, \
            waypoint_tform_body_qy, waypoint_tform_body_qz, waypoint_tform_body_qw. File metadata keys: {metadata.keys()}"
        )
        return None
    waypoint_tform_body = SE3Pose(
        x=float(x), y=float(y), z=float(z), rot=Quat(w=float(qw), x=float(qx), y=float(qy), z=float(qz))
    )
    return waypoint_tform_body


def get_waypoint_and_pose(
    waypoint_id: str, client: CogniteClient
) -> tuple[Optional[dict[str, Any]], Optional[SE3Pose]]:
    """Get a waypoint from the robotics api and return the waypoint and pose."""
    get_waypoints_body = {"items": [{"externalId": waypoint_id}]}
    get_waypoints_response = client.post(
        f"/api/v1/projects/{client.config.project}/robotics/waypoints/byids", json=get_waypoints_body
    )
    waypoint = json.loads(get_waypoints_response.content)["items"]
    if len(waypoint) != 1:
        logger.error(
            f"Failed to process threesixty image. Did not get exactly 1 waypoint with external id {waypoint_id}."
        )
        return None, None
    waypoint_pos = waypoint[0].get("position")
    waypoint_ori = waypoint[0].get("orientation")
    ko_tform_waypoint = SE3Pose(
        x=waypoint_pos["x"],
        y=waypoint_pos["y"],
        z=waypoint_pos["z"],
        rot=Quat(w=waypoint_ori["w"], x=waypoint_ori["x"], y=waypoint_ori["y"], z=waypoint_ori["z"]),
    )
    return waypoint[0], ko_tform_waypoint


def create_and_upload_360_files(
    cognite_client: CogniteClient,
    cognite_threesixty_image_extractor: CogniteThreeSixtyImageExtractor,
    robot_pose: SE3Pose,
    image: Image,
    waypoint: dict[str, Any],
    timestamp: int,
):
    """Create Cogntie three sixty images."""
    rot = Rotation.from_quat(
        [
            robot_pose.rotation.x,
            robot_pose.rotation.y,
            robot_pose.rotation.z,
            robot_pose.rotation.w,
        ]
    )
    rot_vec = rot.as_rotvec()
    rot_angle = np.linalg.norm(rot_vec)
    rot_vec = rot_vec / rot_angle

    event, files = cognite_threesixty_image_extractor.create_threesixty_image(
        content=np.array(image),
        site_id=waypoint.get("mapExternalId", "default_location"),
        site_name=waypoint.get("mapExternalId", "default_location"),
        station_number=waypoint.get("externalId"),  # TODO: make station id more readable
        rotation_angle=str(rot_angle),
        rotation_axis=VectorXYZ(float(rot_vec[0]), float(rot_vec[1]), float(rot_vec[2])),
        rotation_angle_unit="rad",
        translation=VectorXYZ(robot_pose.x, robot_pose.y, robot_pose.z),
        translation_unit="m",
        translation_offset_mm=VectorXYZ(0, 0, 0),
        timestamp=timestamp,
    )
    logger.info("Created event and files")

    # Upload event
    try:
        cognite_client.events.create([event])
    except CogniteDuplicatedError:
        logger.warning(f"Event with external id {event.external_id} already exists. will update event.")
        cognite_client.events.update([event])

    logger.info(f"Created event in CDF with external id {event.external_id}")
    # Upload files
    for file in files:
        cognite_client.files.upload_bytes(
            content=file.content,
            name=file.file_metadata.name,
            data_set_id=file.file_metadata.data_set_id,
            external_id=file.file_metadata.external_id,
            source=file.file_metadata.source,
            mime_type=file.file_metadata.mime_type,
            metadata=file.file_metadata.metadata,
            asset_ids=file.file_metadata.asset_ids,
            labels=file.file_metadata.labels,
            overwrite=True,
        )
        logger.info(f"Created file in CDF with external id {file.file_metadata.external_id}")
    logger.info("Completed uploading 360 image to CDF.")


def process_threesixty_files(files: FileMetadataList, client: CogniteClient, data_set_id: int):
    """Process three sixty images."""
    cognite_threesixty_image_extractor = CogniteThreeSixtyImageExtractor(data_set_id=data_set_id)

    file: FileMetadata
    for file in files:
        try:
            # Updating the file immediately so that the same file will not be processed again
            client.files.update(FileMetadataUpdate(id=file.id).labels.remove("threesixty"))

            logger.info(f"Processing file with external id {file.external_id}, id {file.id}")
            if not file.uploaded:
                logger.error(f"file not upload not completed {file.external_id}")
                continue

            # Get waypoint id from metadata
            waypoint_id = file.metadata.get("waypoint_id")
            print(f"Waypoint id: {waypoint_id}")
            if not waypoint_id:
                client.files.update(FileMetadataUpdate(id=file.id).labels.remove("threesixty"))
                logger.error(f"Failed to process file {file.external_id}. No waypoint id in the metadata.")
                continue

            # Calculate robot pose
            waypoint_tform_body = convert_metadata_to_se3_pose(file.external_id, file.metadata)
            if waypoint_tform_body is None:
                continue
            waypoint, ko_tform_waypoint = get_waypoint_and_pose(waypoint_id=waypoint_id, client=client)
            if waypoint_tform_body is None or waypoint is None:
                continue
            robot_pose = ko_tform_waypoint * waypoint_tform_body

            print(f"waypoint : {waypoint}")

            site_alignement = get_map_transform_from_map(
                client=client, map_external_id=str(waypoint.get("mapExternalId"))
            )

            site_pose = site_alignement * robot_pose

            # Download file
            image_data = client.files.download_bytes(id=file.id)
            try:
                image = Image.open(io.BytesIO(image_data))
            except Exception:
                logger.error(f"This CDF File does not seem to be an image. File ID: {file.id}")
                continue

            # If images don't have a timestamp metadata field, default to the created time of the image file
            image_timestamp = file.metadata.get("timestamp")
            if image_timestamp is None:
                image_timestamp = file.created_time

            # Create and upload 360 files
            create_and_upload_360_files(
                cognite_client=client,
                cognite_threesixty_image_extractor=cognite_threesixty_image_extractor,
                robot_pose=site_pose,
                image=image,
                waypoint=waypoint,
                timestamp=image_timestamp,
            )
        except Exception as e:
            client.files.update(FileMetadataUpdate(id=file.id).labels.add("threesixty"))
            logger.error(f"Failed to process file {file}. Error: {e}")


def handle(data, client):
    """Three sixty image handle. ."""
    logger.info("Start three sixty processing.")

    # Check that input contains data_set_id
    if "data_set_external_id" not in data.keys():
        raise RuntimeError("Data should contain all keys: data_set_id")
    data_set_id = client.data_sets.retrieve(external_id=data["data_set_external_id"]).id

    # Get all 360 images in the data set id with the label "threesixty"
    # Changed default limit to 1 to process 1 file at a time as the upload_queue in cognite_threesixty_image uploads the whole queue
    # on every file but after the first cycle fails as the first event is duplicated and the whole queue upload fails
    files: FileMetadataList = client.files.list(
        labels=LabelFilter(contains_all=["threesixty"]),
        limit=25,
        data_set_ids=[data_set_id],
        uploaded=True,
    )
    logger.info(f"Processing {len(files)} threesixty files.")
    # Process three sixty files
    process_threesixty_files(files=files, client=client, data_set_id=data_set_id)
    return {}
