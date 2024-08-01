"""Contextualize handler."""

import logging
import sys

from cognite.client.data_classes import FileMetadataUpdate, TimeSeries
from common.cdf_helpers import create_missing_labels

MAX_GET_JOB_ATTEMPTS = 10
logger = logging.getLogger(__name__)


def get_time_series_label(camera_type):
    if camera_type == "ir":
        time_series_label = "IR_TIME_SERIES"
    else:
        time_series_label = "ANALOG_GAUGE_TIME_SERIES"
    return time_series_label


def has_label(label_list, label_external_id):
    """Check if the label_external_id is in a list of labels."""
    for label in label_list:
        if label.external_id == label_external_id:
            return True
    return False


def create_timeseries(
    client,
    asset,
    data_set_external_id,
    time_series_label,
    gauge_number=1,
    total_number_of_gauges=1,
    time_series_naming_tag="",
):
    """Create time series on the correct format."""
    if time_series_label == "IR_TIME_SERIES":
        ts_external_id = (
            f"{time_series_label}_{asset.external_id}_{time_series_naming_tag}"
            if asset.external_id is not None
            else f"{asset.id}_{time_series_label}_{time_series_naming_tag}"
        )
        ts_name = f"{asset.name}_ir_{time_series_naming_tag}"

    elif gauge_number <= 1 and total_number_of_gauges <= 1:
        ts_external_id = (
            f"{time_series_label}_{asset.external_id}_0"
            if asset.external_id is not None
            else f"{asset.id}_{time_series_label}_0"
        )
        ts_name = f"{asset.name}_gauge_values"
    else:
        ts_external_id = (
            f"{time_series_label}_{asset.external_id}_{gauge_number}"
            if asset.external_id is not None
            else f"{asset.id}_{time_series_label}_{gauge_number}"
        )
        ts_name = f"{asset.name}_gauge_values_{gauge_number}"

    logger.info(f"Creates timeseries with name: {ts_name}")
    data_set_id = client.data_sets.retrieve(external_id=data_set_external_id).id
    return client.time_series.create(
        TimeSeries(
            metadata={"TYPE": time_series_label},
            asset_id=asset.id,
            external_id=ts_external_id,
            data_set_id=data_set_id,
            name=ts_name,
        )
    )


def get_asset(client, file):
    """Get asset from metadata field."""
    if file.metadata.get("asset_id") not in [None, "0", "None"]:
        try:
            asset_id = int(file.metadata["asset_id"])
            asset = client.assets.retrieve(asset_id)
        except Exception as e:
            logger.exception(e)
            return None
        return asset


def get_timeseries_external_id(client, asset, data_set_external_id, time_series_label, number_of_gauges=1):
    """Get time series external id."""
    ts = client.time_series.list(metadata={"TYPE": time_series_label}, asset_ids=[asset.id])
    number_of_timeseries = len(ts)
    ts_external_ids = []
    logger.info(f"{ts}")

    # Create one timeseries for min temperature and one for max temperature for IR images
    if time_series_label == "IR_TIME_SERIES":
        if number_of_timeseries == 0:
            print("CREATE IR TIMESERIES")
            ir_ts_naming_tags = ["min", "max"]
            for naming_tag in ir_ts_naming_tags:
                ts = create_timeseries(
                    client,
                    asset,
                    data_set_external_id,
                    time_series_label,
                    time_series_naming_tag=naming_tag,
                )
                ts_external_ids.append(ts.external_id)
        else:
            ts_external_ids = [ts[i].external_id for i in range(number_of_timeseries)]

    # Logic for creating gauge timeseries
    elif number_of_timeseries >= 1 and number_of_gauges == number_of_timeseries:
        ts_external_ids = [ts[i].external_id for i in range(number_of_timeseries)]
    elif number_of_timeseries >= 1 and number_of_gauges > number_of_timeseries:
        ts_external_ids.append(ts[0].external_id)
        diff = number_of_gauges - number_of_timeseries
        for gauge_number in range(diff):
            logger.info("CREATE GAUGE TIMESERIES")
            new_gauge_number = gauge_number + number_of_timeseries
            ts = create_timeseries(
                client, asset, data_set_external_id, time_series_label, new_gauge_number, number_of_gauges
            )
            ts_external_ids.append(ts.external_id)
    elif number_of_timeseries == 0:
        logger.info("CREATE GAUGE TIMESERIES")
        if number_of_gauges <= 1:
            ts = create_timeseries(client, asset, data_set_external_id, time_series_label)
            ts_external_ids.append(ts.external_id)
        else:
            for gauge_number in range(number_of_gauges):
                ts = create_timeseries(
                    client, asset, data_set_external_id, time_series_label, gauge_number, number_of_gauges
                )
                ts_external_ids.append(ts.external_id)

    return ts_external_ids


def get_number_of_gauges(client, asset):
    """Get the total number of gauges associated with an asset."""
    try:
        asset_data = client.assets.retrieve(id=asset.id)
        asset_metadata = asset_data.metadata
        if "number_of_gauges" in asset_metadata:
            return int(asset_metadata["number_of_gauges"])
        else:
            return 1
    except Exception as e:
        logger.exception(e)
        return 1


def handle(data, client):
    """Contextualize robot data."""
    print("Start gauge reading.")
    required_input_data_fields = {
        "data_set_external_id",
        "read_dial_gauge_label",
        "read_multiple_dial_gauges_label",
        "read_digital_gauge_label",
        "read_level_gauge_label",
        "read_valve_label",
        "read_ir_raw_label",
        "spill_detection_label",
        "gauge_context_label",
    }
    if not required_input_data_fields <= data.keys():
        raise RuntimeError(f"Data should contain all keys:  {required_input_data_fields}. Current data: {data}")

    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=logging.BASIC_FORMAT)
    create_missing_labels(
        client,
        [
            data["read_dial_gauge_label"],
            data["read_multiple_dial_gauges_label"],
            data["read_digital_gauge_label"],
            data["read_level_gauge_label"],
            data["read_valve_label"],
            data["read_ir_raw_label"],
            data["spill_detection_label"],
            "threesixty",
        ],
    )
    files = client.files.list(
        data_set_external_ids=[data["data_set_external_id"]],
        metadata={"processed": "false"},
        limit=-1,
        uploaded=True,
    )

    logger.info(f"Number of files to process in dataset  {data['data_set_external_id']}: {len(files)}")
    print(f"Number of files to process in dataset  {data['data_set_external_id']}: {len(files)}")

    for file in files:
        number_of_gauges = 1  # Default is one gauge per image
        metadata = file.metadata
        logger.info(f"Processing file with external id: {file.external_id}, id: {file.id}")

        update = FileMetadataUpdate(file.id)
        asset = get_asset(client, file)
        update.asset_ids.add([asset.id]) if asset else None

        print(f"The labels on this file are: {file.labels}")
        if file.labels and has_label(file.labels, data["gauge_context_label"]):
            gauge_type = metadata.get("gauge_type", "dial")  # Default gauge type from robot is dial
            print(f"This file has gauge type: {gauge_type}")

            camera_type = metadata.get("camera", None)
            time_series_label = get_time_series_label(camera_type)

            if gauge_type == "valve":
                current_action_label = "read_valve_label"
            elif gauge_type == "dial":
                if asset:
                    number_of_gauges = get_number_of_gauges(client, asset)
                if number_of_gauges <= 1:
                    current_action_label = "read_dial_gauge_label"
                else:
                    current_action_label = "read_multiple_dial_gauges_label"
            else:
                current_action_label = f"read_{gauge_type}_gauge_label"

            logger.info(f"Setting action label to {data[current_action_label]}")
            print(f"Setting action label to {data[current_action_label]}")

            update.labels.add(data[current_action_label])
            update.labels.remove(data["gauge_context_label"])

            if asset and gauge_type != "spill":
                logger.info(f"Number of gauges: {number_of_gauges}.")
                ts_eid = get_timeseries_external_id(
                    client, asset, data["data_set_external_id"], time_series_label, number_of_gauges
                )
                logger.info(f"Time series linked to asset: {ts_eid}.")
                if ts_eid:
                    if time_series_label == "IR_TIME_SERIES":
                        for i in range(len(ts_eid)):
                            metadata[f"ts_external_id_{i}"] = ts_eid[i]
                    elif number_of_gauges <= 1:
                        metadata["ts_external_id"] = ts_eid[0]
                    else:
                        for gauge_number in range(number_of_gauges):
                            if metadata.get("ts_external_id", None):
                                if gauge_number == 0:
                                    continue
                                else:
                                    metadata[f"ts_external_id_{gauge_number}"] = ts_eid[gauge_number]
                            else:
                                metadata[f"ts_external_id_{gauge_number}"] = ts_eid[gauge_number]
        elif "threesixty" in file.external_id:
            print("This is a 360 image, adding threesixty label.")
            update.labels.add(["threesixty"])

        elif asset and metadata.get("method") == "process_ir_raw":
            current_action_label = "read_ir_raw_label"
            time_series_label = "IR_TIME_SERIES"
            print(f"this is the asset: {asset}")

            ts_eid = get_timeseries_external_id(
                client, asset, data["data_set_external_id"], time_series_label, number_of_gauges
            )
            if time_series_label == "IR_TIME_SERIES":
                for i in range(len(ts_eid)):
                    metadata[f"ts_external_id_{i}"] = ts_eid[i]

        metadata["processed"] = "true"
        update.metadata.add(metadata)
        client.files.update(update)
    return {}
