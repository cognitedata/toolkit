from __future__ import annotations

import logging
import time
from typing import Any, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import Annotation, LabelDefinition

logger = logging.getLogger(__name__)


def call_vision_api_endpoint(
    client: CogniteClient,
    url: str,
    input_data: dict,
    max_get_job_attempts: int = 20,
    headers: Optional[dict[str, Any]] = None,
):
    """Post to an API endpoint, wait for response and return response."""
    # TODO: Use this function in people detector function (https://cognitedata.atlassian.net/browse/DMVP-855)
    res = client.post(url=url, json=input_data, headers=headers).json()

    job_id = res.get("jobId")
    for i in range(max_get_job_attempts):
        logger.info(f"Attempt nr. {i} to get job status.")
        res = client.get(url=f"{url}/{job_id}").json()
        if res.get("status") in ["Queued", "Running"]:
            logger.info(f"API job status: {res.get('status')}")
            time.sleep(10)
        elif res.get("status") == "Completed":
            logger.info("API job completed")
            break
    else:  # Use for/else in case of failed job or timeout
        logger.info(f"API job failed or timed out: {res}")
        return None

    return res


def create_missing_labels(client: CogniteClient, label_ids: list[str]):
    """Create missing labels from a list of label IDs."""
    for label_id in label_ids:
        if not check_label_exists(client, label_id):
            client.labels.create([LabelDefinition(external_id=label_id, name=label_id)])


def check_label_exists(client: CogniteClient, label_id: str):
    """Check if a label exists in CDF given its external_id (label_id)."""
    labels = client.labels.list(external_id_prefix=label_id)
    return any(label.external_id == label_id for label in labels)


def create_annotations(
    client: CogniteClient,
    gauge_reading_result: dict,
    file_id: int,
    gauge_type: str,
    bounding_box_label: str,
    keypoint_label: str,
):
    annotations = gauge_reading_result["items"][0]["predictions"][gauge_type + "GaugePredictions"]
    annotations_list = []

    for annotation in annotations:
        print(f"ANNOTATION TYPE: {annotation}")
        if gauge_type == "digital":
            bounding_box = annotation["boundingBox"]
        else:
            keypoints = annotation["keypointCollection"]["keypoints"]
            bounding_box = annotation["objectDetection"]["boundingBox"]
        # Create bounding box annotation
        bounding_box_annotation = Annotation(
            annotation_type="images.ObjectDetection",
            status="suggested",
            creating_user="cognite-functions",
            creating_app="sdk",
            creating_app_version="4.5.2",
            annotated_resource_type="file",
            annotated_resource_id=file_id,
            data={
                "label": bounding_box_label,
                "boundingBox": bounding_box,
            },
        )
        annotations_list.append(bounding_box_annotation)

        if gauge_type in ["level", "dial"]:
            # Create keypoint annotation
            keypoint_names = list(keypoints.keys())
            keypoint_data = {}
            for i in range(len(keypoint_names)):
                keypoint_name = keypoint_names[i]
                print(keypoint_name)
                point = keypoints[keypoint_name]["point"]
                print(point)
                keypoint_data[keypoint_name] = {"point": point}
                print(keypoint_data)

            keypoint_annotation = Annotation(
                annotation_type="images.KeypointCollection",
                status="suggested",
                creating_user="cognite-functions",
                creating_app="sdk",
                creating_app_version="4.5.2",
                annotated_resource_type="file",
                annotated_resource_id=file_id,
                data={"label": keypoint_label, "keypoints": keypoint_data},
            )
            annotations_list.append(keypoint_annotation)

    client.annotations.create(annotations_list)
