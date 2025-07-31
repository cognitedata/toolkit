from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AssetCentricData:
    data_set_id: tuple[int, ...] | None = None
    hierarchy: tuple[int, ...] | None = None

    def as_filter(self) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=list(self.data_set_id) if self.data_set_id else None,
            asset_subtree_external_ids=list(self.hierarchy) if self.hierarchy else None,
        )
