from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._cognite_client import ClientConfig, CogniteClient

from .statistics import StatisticsAPI


class ExtendedDataModelingAPI(DataModelingAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.statistics = StatisticsAPI(config, api_version, cognite_client)
