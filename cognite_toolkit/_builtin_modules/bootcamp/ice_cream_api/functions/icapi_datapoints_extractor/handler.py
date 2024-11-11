import os
from ast import literal_eval
from datetime import datetime, timedelta, timezone
from threading import Event

from cognite.client import CogniteClient
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from config import Config
from ice_cream_factory_api import IceCreamFactoryAPI


def get_timeseries_for_site(client: CogniteClient, site, config: Config):
    this_site = site.lower()
    ts = client.time_series.list(
        data_set_external_ids=config.extractor.data_set_ext_id, metadata={"site": this_site}, limit=None
    )

    # filter returned list because the API returns connected timeseries. planned_status -> status, good -> count
    ts = [item for item in ts if any(substring in item.external_id for substring in ["planned_status", "good"])]
    return ts


def run_extractor(client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event) -> None:
    # The only way to pass variables to and Extractor's run function
    if "SITES" in os.environ:
        config.extractor.sites = literal_eval(os.environ["SITES"])
    if "BACKFILL" in os.environ:
        config.extractor.backfill = True if os.environ["BACKFILL"] == "True" else False
        if "HOURS" in os.environ:
            config.extractor.hours = int(os.environ["HOURS"])

    now = datetime.now(timezone.utc).timestamp() * 1000
    increment = timedelta(seconds=7200).total_seconds() * 1000

    ice_cream_api = IceCreamFactoryAPI(base_url=config.extractor.api_url)

    upload_queue = TimeSeriesUploadQueue(
        client,
        post_upload_function=states.post_upload_handler(),
        max_queue_size=500000,
        trigger_log_level="INFO",
        thread_name="Timeseries Upload Queue",
    )

    for site in config.extractor.sites:
        print(f"Getting TimeSeries for {site}")
        time_series = get_timeseries_for_site(client, site, config)

        if not config.extractor.backfill:
            # Get all the latest datapoints in one API call
            latest_dps = {
                dp.external_id: dp.timestamp
                for dp in client.time_series.data.retrieve_latest(
                    external_id=[ts.external_id for ts in time_series], ignore_unknown_ids=True
                )
            }

        for ts in time_series:
            # figure out the window of datapoints to pull
            if not config.extractor.backfill:
                latest = latest_dps[ts.external_id][0] if latest_dps.get(ts.external_id) else None
                start = latest if latest else now - increment
            else:
                start = now - timedelta(hours=config.extractor.hours).total_seconds() * 1000
            end = now

            dps = ice_cream_api.get_datapoints(timeseries_ext_id=ts.external_id, start=start, end=end)
            for external_id, datapoints in dps.items():
                upload_queue.add_to_upload_queue(external_id=external_id, datapoints=datapoints)

            print(f"Queued {len(datapoints)} {ts.external_id} datapoints for upload")

        # trigger upload for this site
        upload_queue.upload()


def handle(client: CogniteClient = None, data=None):
    config_file_path = "extractor_config.yaml"

    # Can't pass parameters to the Extractor, so create environment variables
    if data:
        sites = data.get("sites")
        backfill = data.get("backfill")
        hours = data.get("hours")

        if sites:
            os.environ["SITES"] = f"{sites}"
        if backfill:
            os.environ["BACKFILL"] = f"{backfill}"
            if hours:
                os.environ["HOURS"] = f"{hours}"

    with Extractor(
        name="icapi_datapoints_extractor",
        description="An extractor that ingest Timeseries' Datapoints from the Ice Cream Factory API to CDF clean",
        config_class=Config,
        version="1.0",
        config_file_path=config_file_path,
        run_handle=run_extractor,
    ) as extractor:
        extractor.run()
