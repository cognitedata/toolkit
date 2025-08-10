from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.client import ToolkitClient


@dataclass(frozen=True)
class AssetCentricData:
    data_set_id: tuple[str, ...] | None = None
    hierarchy: tuple[str, ...] | None = None

    def as_filter(self, client: ToolkitClient) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=client.lookup.data_sets.id(self.data_set_id) if self.data_set_id else None,
            asset_subtree_external_ids=client.lookup.assets.id(self.hierarchy) if self.hierarchy else None,
        )
