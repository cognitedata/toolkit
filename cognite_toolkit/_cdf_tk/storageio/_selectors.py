from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AssetCentricData:
    """Data class for asset-centric data selection.

    This class is used to filter asset-centric data based on a data set external ID and a hierarchy.

    Args:
        data_set_external_id (str | None): The external ID of the data set to filter by. (Used for download)
        hierarchy (str | None): The hierarchy to filter by, typically an asset subtree external ID. (Used for download)
        datafile (Path | None): The path to the data file associated with this selection. (Used for upload)

    """

    data_set_external_id: str | None = None
    hierarchy: str | None = None
    datafile: Path | None = None

    def as_filter(self) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=[self.data_set_external_id] if self.data_set_external_id else None,
            asset_subtree_external_ids=[self.hierarchy] if self.hierarchy else None,
        )
