from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataUpdate, LabelFilter
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from common.cdf_helpers import create_missing_labels

IMAGE_HEIGHT = 512
IMAGE_WIDTH = 640

logger = logging.getLogger(__name__)


def get_timestamp(file):
    """Get timestamp from file."""
    timestamp = file.metadata.get("timestamp")
    if not timestamp:
        timestamp = file.source_created_time
    if not timestamp:
        # This definitely exists
        timestamp = file.uploaded_time
    return timestamp


def get_asset(client, file):
    """Get asset from metadata field."""
    if file.metadata.get("asset_id", None) not in [None, "0", "None"]:
        try:
            asset_id = int(file.metadata["asset_id"])
        except Exception as e:
            logger.exception(e)
            return None
        return client.assets.retrieve(asset_id)


def handle_failed_upload(client: CogniteClient, id: int, error_message: str, data: dict, metadata: dict | None = None):
    """Log error message and update a file that has failed."""
    print(error_message)

    client.files.update(
        FileMetadataUpdate(id=id)
        .labels.remove(data["input_label"])
        .labels.add([data["output_label"], data["failed_label"]])
        .metadata.add(
            {"error_message": error_message, **metadata} if metadata is not None else {"error_message": error_message}
        )
    )


def handle(data, client):
    print("Start extracting data from the IR raw file.")

    if not {"input_label", "output_label", "success_label", "failed_label", "data_set_external_id"} <= data.keys():
        raise RuntimeError(
            "Data should contain all keys: 'input_label', 'output_label', 'success_label', 'failed_label', 'data_set_external_id'."
        )

    data_set_id = client.data_sets.retrieve(external_id=data["data_set_external_id"]).id

    files = client.files.list(
        labels=LabelFilter(contains_all=[data["input_label"]]),
        limit=-1,
        uploaded=True,
    )

    print(f"Number of files to process with label: {data['input_label']}: {len(files)}.")

    create_missing_labels(client=client, label_ids=[data["output_label"], data["success_label"], data["failed_label"]])

    upload_queue = TimeSeriesUploadQueue(
        client,
        max_upload_interval=1,
        create_missing=True,
        max_queue_size=100,
    )

    for file in files:
        if file.metadata.get("processed") == "false":
            print(f"File with external id: {file.external_id} not processed. Skipping.")
            continue

        ir_image_filename = "ir_image.jpg"
        ir_temperature_filename = "ir_temperatures.csv"
        ir_raw_filename = "ir_raw.raw"

        asset = get_asset(client, file)

        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            ir_image_path = str(Path.cwd() / directory / ir_image_filename)
            ir_temperature_path = str(Path.cwd() / directory / ir_temperature_filename)
            ir_raw_path = str(Path.cwd() / directory / ir_raw_filename)

            # Download raw file to tmp path
            client.files.download_to_path(path=ir_raw_path, id=file.id)

            # Extract temperatures from the raw file
            temperatures_decikelvin = np.fromfile(file=ir_raw_path, dtype=np.uint16).byteswap()

            # Reshape the raw temperature array
            temperatures_decikelvin = temperatures_decikelvin.reshape((IMAGE_HEIGHT, IMAGE_WIDTH))

            # Convert degrees decikelvin to degrees celsius
            temperatures_celsius = ((temperatures_decikelvin) / 10) - 273.15

            # Save the temperatures to a temp csv file
            np.savetxt(ir_temperature_path, temperatures_celsius, delimiter=",")

            # Save the image to the temp path
            plt.imsave(ir_image_path, temperatures_celsius)

            # Upload image and temperature data to CDF
            try:
                res_image = client.files.upload(
                    path=ir_image_path,
                    external_id=f"ir_image_{file.name}_{file.external_id}_{file.uploaded_time}",
                    name=f"ir_image_{file.external_id}.jpg",
                    data_set_id=data_set_id,
                    mime_type="image/jpeg",
                    asset_ids=[asset.id],
                    metadata={
                        "asset_id": file.metadata.get("asset_id", None),
                        "raw_file_id": file.id,
                        "raw_file_name": file.name,
                    },
                )
                file_update = FileMetadataUpdate(id=file.id).metadata.add({"ir_image_id": res_image.id})
                client.files.update(file_update)

            except Exception as e:
                handle_failed_upload(client=client, id=file.id, error_message=str(e), data=data, metadata=file.metadata)
                continue

            print(f"Uploaded IR image with ID: {res_image.id}.")

            try:
                res_csv = client.files.upload(
                    path=ir_temperature_path,
                    external_id=f"temperatures_{file.name}_{file.external_id}_{file.uploaded_time}",
                    name=f"temperatures_{file.external_id}.csv",
                    data_set_id=data_set_id,
                    mime_type="text/csv",
                    asset_ids=[asset.id],
                    metadata={
                        "asset_id": file.metadata.get("asset_id", None),
                        "raw_file_id": file.id,
                        "raw_file_name": file.name,
                    },
                )
                file_update = FileMetadataUpdate(id=file.id).metadata.add({"ir_temp_csv_id": res_csv.id})
                client.files.update(file_update)

            except Exception as e:
                handle_failed_upload(client=client, id=file.id, error_message=str(e), data=data, metadata=file.metadata)
                continue

            print(f"Uploaded temperature file with ID: {res_csv.id}.")

            # Write the minimum and maximum temperature to the corresponding timeseries
            minimum_temperature = np.amin(temperatures_celsius)
            maximum_temperature = np.amax(temperatures_celsius)

            # if "ts_external_id" in file.metadata:
            if any("ts_external_id" in key for key in file.metadata):
                ts_external_ids = list(value for key, value in file.metadata.items() if "ts_external_id" in key)
                ts_external_ids = sorted(ts_external_ids)

                timestamp = int(get_timestamp(file))

                for ts_eid in ts_external_ids:
                    temp = maximum_temperature if ts_eid.endswith("max") else minimum_temperature
                    eid = ts_eid

                    print(f"Datapoint {temp} at timestamp {timestamp} written to timeseries {ts_eid}.")

                    upload_queue.add_to_upload_queue(
                        datapoints=[(timestamp, temp)],
                        external_id=eid,
                    )

        client.files.update(
            FileMetadataUpdate(id=file.id)
            .labels.remove(data["input_label"])
            .labels.add([data["output_label"], data["success_label"]])
        )

        print(f"IR reading completed successfully for file with external id: {file.external_id}, and id: {file.id}.")

    upload_queue.upload()
    return {}
