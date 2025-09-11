from abc import ABC, abstractmethod
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.commands._migrate.adapter import MigrationSelector, AssetCentricMappingList, \
    AssetCentricMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.issues import MigrationIssue

class DataMapper(Generic[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceList], ABC):
    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map_chunk(self, source: T_WritableCogniteResourceList) -> tuple[T_CogniteResourceList, list[MigrationIssue]]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A tuple containing the mapped data and a list of any issues encountered during mapping.

        """
        raise NotImplementedError("Subclasses must implement this method.")




class AssetCentricMapper(DataMapper[MigrationSelector, AssetCentricMappingList, InstanceApplyList]):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    def prepare(self, source_selector: MigrationSelector) -> None:
        ...


    def map_chunk(self, source: AssetCentricMappingList) -> tuple[InstanceApplyList, list[MigrationIssue]]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        instances = InstanceApplyList([])
        issues: list[MigrationIssue] = []
        item: AssetCentricMapping
        for item in source:
            mapping: MigrationMapping = item.mapping
            instance, conversion_issue = asset_centric_to_dm(
                item.resource,
                instance_id=mapping.instance_id,
                view_source=,
                view_properties=,
            )
            instances.append(instance)
            issues.append(conversion_issue)
        return instances, issues

