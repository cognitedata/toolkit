from threading import Event

from cognite.client import CogniteClient
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from config import Config
from ice_cream_factory_api import IceCreamFactoryAPI


def run_extractor(
    client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event
) -> None:

    ice_cream_api = IceCreamFactoryAPI(base_url=config.extractor.api_url)
    time_series = ice_cream_api.get_timeseries()

    # add the dataset to all TimeSeries
    data_set = client.data_sets.retrieve(external_id=config.extractor.data_set_ext_id)
    if not data_set:
        stop_event.set()
        print(f"Data set {config.extractor.data_set_ext_id} not found")

    for ts in time_series:
        ts.data_set_id = data_set.id

    client.time_series.upsert(item=time_series)

def handle(client: CogniteClient = None, data = None):
    if data:
        config_file_path = data.get("config_file_path", "extractor_config.yaml")
    else:
        config_file_path = "extractor_config.yaml"

    with Extractor(
        name="icapi_timeseries_extractor",
        description="An extractor that ingests Time Series metadata from the Ice Cream Factory API to CDF clean",
        config_class=Config,
        version="1.0",
        config_file_path=config_file_path,
        run_handle=run_extractor,
    ) as extractor:
        extractor.run()

if __name__ == "__main__":
    handle()
