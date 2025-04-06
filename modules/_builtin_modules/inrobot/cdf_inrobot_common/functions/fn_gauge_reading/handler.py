"""Gauge reader handler."""

from __future__ import annotations

import logging
import sys
from typing import Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataUpdate, LabelFilter, TimeSeriesUpdate
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from common.apm_helpers import _get_position_from_metadata_to_vec3f, create_apm_observation_from_reading
from common.cdf_helpers import call_vision_api_endpoint, create_annotations, create_missing_labels

MAX_GET_JOB_ATTEMPTS = 20

METADATA = {
    "dial": {"required": [], "all": ["min_level", "max_level", "dead_angle"]},
    "digital": {
        "required": ["comma_pos"],
        "all": ["comma_pos", "min_num_digits", "max_num_digits"],
    },
    "level": {"required": [], "all": ["min_level", "max_level"]},
    "valve": {"required": [], "all": []},
}
SUPPORTED_GAUGES = ["dial", "digital", "level", "valve"]
GAUGE_FEATURE_TYPES = {
    "dial": "DialGaugeDetection",
    "digital": "DigitalGaugeDetection",
    "level": "LevelGaugeDetection",
    "valve": "ValveDetection",
}

GAUGE_PARAMETERS_TYPES = {
    "dial": "dialGaugeDetectionParameters",
    "digital": "digitalGaugeDetectionParameters",
    "level": "levelGaugeDetectionParameters",
    "valve": "valveDetectionParameters",
}

logger = logging.getLogger(__name__)

BOUNDING_BOX_LABELS = {"dial": "dial-gauge", "digital": "digital-gauge", "level": "level-gauge", "valve": "valve"}
KEYPOINT_LABELS = {
    "dial": "dial-gauge-keypoints",
    "digital": "digital-gauge-keypoints",
    "level": "level-gauge-keypoints",
    "valve": "valve-keypoints",
}


def update_ts_metadata(client, file_metadata, metadata_keys):
    """Update a timeseries with gauge reading metadata."""
    metadata = {key: file_metadata.get(key) for key in metadata_keys}
    logger.info(f"Timeseries {file_metadata['ts_external_id']} updated with metadata {metadata}")
    client.time_series.update(TimeSeriesUpdate(external_id=file_metadata["ts_external_id"]).metadata.add(metadata))


def gauge_reading_attributes_from_response(res: dict, gauge_type: str) -> Optional[dict]:
    """Return the gauge reading annotations from the API response."""
    if not res:
        logger.error("API call failed. Did not get a response.")
        return None
    if not res["items"]:
        logger.error(f"No items in result: {res}")
        return None

    if gauge_type == "valve":
        valve_readings = [annotation for annotation in res["items"][0]["predictions"]["valvePredictions"]]
        if not valve_readings:
            logger.error(f"No annotation of type {gauge_type} in result.")
            return None
        for reading in valve_readings:
            if reading.get("keypointCollection", None):
                data = reading["keypointCollection"]
    else:
        gauge_readings = [annotation for annotation in res["items"][0]["predictions"][gauge_type + "GaugePredictions"]]
        gauge_attributes = []

        for gauge_reading in gauge_readings:
            if gauge_type == "digital":
                if gauge_reading.get("attributes", None):
                    gauge_attributes.append(gauge_reading)
            else:
                if gauge_reading.get("keypointCollection", None):
                    gauge_attributes.append(gauge_reading["keypointCollection"])

        if not gauge_attributes:
            logger.error(f"No annotation of type {gauge_type} in result.")
            return None

        if "attributes" not in gauge_attributes[0]:
            logger.error("No attributes in result.")
            return None
        data = gauge_attributes[0]["attributes"]

    return data


def handle_failed_upload(client: CogniteClient, id: int, error_message: str, data: dict, metadata: dict | None = None):
    """Log error message and update a file that has failed."""
    logger.error(error_message)

    if data["gauge_type"] == "valve":
        client.files.update(
            FileMetadataUpdate(id=id)
            .labels.remove(data["input_label"])
            .labels.add([data["output_label"], data["failed_label"]])
            .metadata.add({"error_message": error_message})
        )
    else:
        client.files.update(
            FileMetadataUpdate(id=id)
            .labels.remove(data["input_label"])
            .labels.add([data["output_label"], data["failed_label"]])
            .metadata.add(
                {"error_message": error_message, **metadata}
                if metadata is not None
                else {"error_message": error_message}
            )
        )


def to_input_metadata(keys: list[str], metadata: dict):
    return {to_camel_case(key): metadata.get(key) for key in keys}


def get_timestamp(file):
    """Get timestamp from file."""
    timestamp = file.metadata.get("timestamp")
    if not timestamp:
        timestamp = file.source_created_time
    if not timestamp:
        # This definitely exists
        timestamp = file.uploaded_time
    return timestamp


def to_camel_case(snake):
    """Convert from snake case to camel case."""
    components = snake.split("_")
    return components[0] + "".join(x.title() for x in components[1:]) if len(components) > 1 else components[0]


def handle(data, client):
    """Gauge reader handle. Only analog, digital and level gauges supported at the moment."""
    if not {"gauge_type", "input_label", "output_label", "success_label", "failed_label"} <= data.keys():
        raise RuntimeError(
            "Data should contain all keys:  'gauge_type', 'input_label', 'output_label', 'success_label', 'failed_label'"
        )

    if data["gauge_type"] not in SUPPORTED_GAUGES:
        raise NotImplementedError(f"Only {SUPPORTED_GAUGES} gauge reading supported, not {data['gauge_type']}")

    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=logging.BASIC_FORMAT)

    files = client.files.list(
        labels=LabelFilter(contains_all=[data["input_label"]]),
        limit=15,
        uploaded=True,
    )
    logger.info(f"Number of files to process with label {data['input_label']}: {len(files)}")

    create_missing_labels(client, [data["output_label"], data["success_label"], data["failed_label"]])
    upload_queue = TimeSeriesUploadQueue(
        client,
        max_upload_interval=1,
        create_missing=True,
        max_queue_size=100,
    )
    project = client.config.project
    for file in files:
        ts_metadata = False
        logger.info(f"Processing file with external id {file.external_id}, id {file.id}")
        file.metadata = {} if file.metadata is None else file.metadata

        # If the file has a timeseries external id, try to get a complete metadata set from the timeseries.
        # If the timeseries does not have all metadata, try to get a complete metadata set from the file.
        if "ts_external_id" in file.metadata:
            ts = client.time_series.retrieve(external_id=file.metadata.get("ts_external_id"))
            logger.info(f"Retrieve timeseries {file.metadata.get('ts_external_id')}")
            if not ts:
                handle_failed_upload(
                    client=client,
                    id=file.id,
                    error_message=f"Time series with metadata {file.metadata.get('ts_external_id')} does not exist",
                    data=data,
                )
                continue
            if ts.metadata and all(key in ts.metadata for key in METADATA[data["gauge_type"]]["all"]):
                metadata = to_input_metadata(METADATA[data["gauge_type"]]["all"], ts.metadata)
                ts_metadata = True
                logger.info(f"Metadata from timeseries: {metadata}")
                for key in METADATA[data["gauge_type"]]["all"]:
                    file.metadata[key] = str(ts.metadata[key])
                    if "unit" in ts.metadata:
                        file.metadata["unit"] = ts.metadata["unit"]

            elif all(key in file.metadata for key in METADATA[data["gauge_type"]]["all"]):
                metadata = to_input_metadata(METADATA[data["gauge_type"]]["all"], file.metadata)
                logger.info(f"Metadata from file: {metadata}")
                update_ts_metadata(client, file.metadata, METADATA[data["gauge_type"]]["all"])
                logger.info(f"Complete metadata from image: {metadata}")
                ts_metadata = True

        if not ts_metadata:
            if not all(key in file.metadata for key in METADATA[data["gauge_type"]]["required"]):
                handle_failed_upload(
                    client=client,
                    id=file.id,
                    error_message=f"Some required metadata fields missing. Required metadata is: {METADATA[data['gauge_type']]['required']}. Cannot process image.",
                    data=data,
                )
                continue

            # If we did not find complete metadata from time series or file, use (possibly incomplete) metadata from file.
            metadata = {}
            for key in METADATA[data["gauge_type"]]["all"]:
                if key in file.metadata:
                    metadata[to_camel_case(key)] = file.metadata[key]
            logger.info(f"Not complete metadata from image: {metadata}")

        # Check gauge_type and make api-call based on the gauge_type
        if data["gauge_type"] == "valve":
            res = call_vision_api_endpoint(
                client=client,
                url=f"/api/v1/projects/{project}/context/vision/extract",
                input_data={"items": [{"fileId": file.id}], "features": ["ValveDetection"]},
                max_get_job_attempts=MAX_GET_JOB_ATTEMPTS,
                headers={"cdf-version": "beta"},
            )
        else:
            res = call_vision_api_endpoint(
                client=client,
                url=f"/api/v1/projects/{project}/context/vision/extract",
                input_data={
                    "items": [{"fileId": file.id}],
                    "features": [GAUGE_FEATURE_TYPES[data["gauge_type"]]],
                    "parameters": {GAUGE_PARAMETERS_TYPES[data["gauge_type"]]: metadata},
                },
                max_get_job_attempts=MAX_GET_JOB_ATTEMPTS,
                headers={"cdf-version": "beta"},
            )

        print(f"RES: {res}")
        if not res:
            handle_failed_upload(
                client=client, id=file.id, error_message="API call failed. Did not get a response", data=data
            )
            continue

        gauge_reading_attributes = gauge_reading_attributes_from_response(res, data["gauge_type"])
        logger.info("-----------------------------GAUGE READING-------------------------------")
        logger.info(f"External id: {file.external_id}, Reading: {gauge_reading_attributes}")
        logger.info("-------------------------------------------------------------------------")
        if gauge_reading_attributes is None:
            handle_failed_upload(
                client=client,
                id=file.id,
                error_message=f"Failed to read gauge {file.external_id}. No gauge found in image.",
                data=data,
            )
            continue
        read_value = (
            gauge_reading_attributes["attributes"]["valveState"]["value"]
            if data["gauge_type"] == "valve"
            else gauge_reading_attributes[data["gauge_type"] + "GaugeValue"]["value"]
        )
        if read_value is None:
            handle_failed_upload(
                client=client,
                id=file.id,
                error_message=f"Failed to read gauge {file.external_id}. Could not read value.",
                data=data,
                metadata=gauge_reading_attributes.get("metadata", None),
            )
            continue

        # If the reading was successful
        if data["gauge_type"] == "valve":
            file.metadata["state"] = read_value
            logger.info(f"Predicted state: {file.metadata['state'] }")

        else:
            # If the readings were successful, draw the annotations on the image
            create_annotations(
                client=client,
                gauge_reading_result=res,
                file_id=file.id,
                gauge_type=data["gauge_type"],
                bounding_box_label=BOUNDING_BOX_LABELS[data["gauge_type"]],
                keypoint_label=KEYPOINT_LABELS[data["gauge_type"]],
            )

            if data["gauge_type"] == "digital":
                file.metadata["value"] = read_value
            else:
                file.metadata["value"] = f"{read_value:.2f}"

            for key in METADATA[data["gauge_type"]]["all"]:
                key_attribute = to_camel_case(key)
                if gauge_reading_attributes is not None:
                    key_object = gauge_reading_attributes.get(key_attribute, None)
                    if key_object is not None:
                        key_val = key_object.get("value", None)
                        if key_val is not None:
                            file.metadata[key] = str(key_val)
                        elif key_object is None and key == "dead_angle":
                            file.metadata[key] = "90"  # Dead angle is not returned when it is 90 (default)
            logger.info(f"Predicted value: {file.metadata['value']}")

        if "ts_external_id" in file.metadata and not ts_metadata:
            # If the reading was successful and timeseries metadata does not exist, add ts metadata from the reading.
            metadata = to_input_metadata(METADATA[data["gauge_type"]]["all"], file.metadata)
            update_ts_metadata(client, file.metadata, METADATA[data["gauge_type"]]["all"])

        if "ts_external_id" in file.metadata:
            # Upload datapoint to timeseries if the file has ts_external_id.
            timestamp = int(get_timestamp(file))
            if data["gauge_type"] == "valve":
                timeseries_value = 1.0 if read_value == "on" else 0.0 if read_value == "off" else -1.0
            else:
                timeseries_value = float(read_value)

            upload_queue.add_to_upload_queue(
                datapoints=[(timestamp, timeseries_value)],
                external_id=file.metadata.get("ts_external_id"),
            )

            try:
                position = _get_position_from_metadata_to_vec3f(file.metadata)
                if data["gauge_type"] in ["dial", "digital", "level"]:
                    # create a observation if value is above or below threshold
                    _value_threshold_max = float(ts.metadata.get("observation_threshold_max", "inf"))
                    _value_threshold_min = float(ts.metadata.get("observation_threshold_min", "-inf"))

                    if timeseries_value < _value_threshold_min:
                        create_apm_observation_from_reading(
                            client=client,
                            file_external_id=file.external_id,
                            timeseries_external_id=file.metadata.get("ts_external_id"),
                            value=timeseries_value,
                            timestamp=timestamp,
                            pose=position,
                            message="WARNING: Value below threshold",
                            apm_checklistitem_external_id=file.metadata.get("action_run_id"),
                        )
                    elif timeseries_value > _value_threshold_max:
                        create_apm_observation_from_reading(
                            client=client,
                            file_external_id=file.external_id,
                            timeseries_external_id=file.metadata.get("ts_external_id"),
                            value=timeseries_value,
                            timestamp=timestamp,
                            pose=position,
                            message="WARNING: Value over threshold",
                            apm_checklistitem_external_id=file.metadata.get("action_run_id"),
                        )
                elif data["gauge_type"] == "valve":
                    _expected_state = ts.metadata.get("expected_valve_state", "no_state_specified")
                    if (_expected_state == "on" or _expected_state == "1.0") and timeseries_value == 0.0:
                        create_apm_observation_from_reading(
                            client=client,
                            file_external_id=file.external_id,
                            timeseries_external_id=file.metadata.get("ts_external_id"),
                            value=timeseries_value,
                            timestamp=timestamp,
                            pose=position,
                            message="WARNING: Valve is off",
                            apm_checklistitem_external_id=file.metadata.get("action_run_id"),
                        )
                    elif (_expected_state == "off" or _expected_state == "0.0") and timeseries_value == 1.0:
                        create_apm_observation_from_reading(
                            client=client,
                            file_external_id=file.external_id,
                            timeseries_external_id=file.metadata.get("ts_external_id"),
                            value=timeseries_value,
                            timestamp=timestamp,
                            pose=position,
                            message="WARNING: Valve is on",
                            apm_checklistitem_external_id=file.metadata.get("action_run_id"),
                        )

            except Exception as e:
                logger.error(f"Failed to create APM observation: {e}")

        client.files.update(
            FileMetadataUpdate(id=file.id)
            .labels.remove(data["input_label"])
            .labels.add([data["output_label"], data["success_label"]])
            .metadata.add(file.metadata)
        )
        logger.info(f"Gauge reading completed successfully for file {file.external_id}, id: {file.id}.")

    upload_queue.upload()
    return {}
