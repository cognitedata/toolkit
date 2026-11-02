from dataclasses import dataclass
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatTableSource

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning


@dataclass
class NeatReadResult:
    space: CrudResource[SpaceId]
    containers: list[CrudResource[ContainerId]]
    views: list[CrudResource[ViewId]]
    node_types: list[CrudResource[NodeId]]
    data_model: CrudResource[DataModelId]


class NeatTableError(Exception):
    pass


class NeatTableReader:
    def __init__(self, neat_yaml: NeatYAML, source: NeatTableSource):
        self.neat_yaml = neat_yaml
        self.source = source
        self._warnings: list[BuildWarning] = []

    def read(self) -> NeatReadResult:
        raise NotImplementedError()

    def get_warnings(self) -> list[BuildWarning]:
        return self._warnings
