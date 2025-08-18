from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AssetCentricData:
    data_set_external_id: str | None = None
    hierarchy: str | None = None

    def as_filter(self) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=[self.data_set_external_id] if self.data_set_external_id else None,
            asset_subtree_external_ids=[self.hierarchy] if self.hierarchy else None,
        )
