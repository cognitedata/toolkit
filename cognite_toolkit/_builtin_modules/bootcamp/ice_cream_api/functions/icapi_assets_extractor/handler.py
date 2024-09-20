from io import StringIO
from threading import Event

import pandas
from cognite.client import CogniteClient
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from config import Config
from ice_cream_factory_api import IceCreamFactoryAPI


def run_extractor(
    client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event
) -> None:
    ice_cream_api = IceCreamFactoryAPI(base_url=config.extractor.api_url)

    sites_csv = ice_cream_api.get_sites_csv()
    sites_df = pandas.read_csv(
        StringIO(sites_csv),
        sep=",",
        usecols=["name", "external_id", "description", "metadata", "parent_external_id"]
    )

    client.raw.rows.insert_dataframe(
        dataframe=sites_df,
        db_name=config.extractor.dest.database,
        table_name=config.extractor.dest.table,
        ensure_parent=True
    )

def handle(client: CogniteClient = None, data = None):
    if data:
        config_file_path = data.get("config_file_path", "extractor_config.yaml")
    else:
        config_file_path = "extractor_config.yaml"

    with Extractor(
        name="icapi_assets_extractor",
        description="An extractor that ingest Assets from the Ice Cream Factory API to CDF clean",
        config_class=Config,
        version="1.0",
        config_file_path=config_file_path,
        run_handle=run_extractor,
    ) as extractor:
        extractor.run()
