from collections.abc import Sequence

from cognite.client.data_classes.data_modeling import NodeList

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, Mapping

from .extended_data_modeling import ExtendedInstancesAPI


class MappingAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._RETRIEVE_LIMIT = 1000

    def retrieve(self, ids: Sequence[AssetCentricId]) -> NodeList[Mapping]:
        raise NotImplementedError()


class MigrationAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self.mapping = MappingAPI(instance_api)
