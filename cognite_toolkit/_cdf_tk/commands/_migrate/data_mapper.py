from abc import ABC, abstractmethod
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)

from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WritableCogniteResourceList


class DataMapper(Generic[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceList], ABC):
    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map_chunk(self, source: T_WritableCogniteResourceList) -> T_CogniteResourceList:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            The mapped data chunk in the target format.

        """
        raise NotImplementedError("Subclasses must implement this method.")
