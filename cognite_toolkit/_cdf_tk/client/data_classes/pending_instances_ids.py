import sys
from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._auxiliary import exactly_one_is_not_none

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass(frozen=True)
class PendingInstanceId(CogniteObject):
    """A pending instance ID is a NodeId that is set on a TimeSeries or FileMetadata that tells
    the respective syncer that when that Node is created, it should be linked to the
    TimeSeries or FileMetadata.
    """

    pending_instance_id: NodeId
    id: int | None = None
    external_id: str | None = None

    def __post_init__(self) -> None:
        if not exactly_one_is_not_none(self.id, self.external_id):
            raise ValueError("Either id or external_id must be set for PendingInstanceId.")

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a PendingInstanceId from a dictionary."""
        return cls(
            pending_instance_id=NodeId.load(resource["pendingInstanceId"]),
            id=resource.get("id"),
            external_id=resource.get("externalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the PendingInstanceId to a dictionary."""
        output: dict[str, Any] = {
            "pendingInstanceId" if camel_case else "pending_instance_id": self.pending_instance_id.dump(
                camel_case=camel_case, include_instance_type=False
            ),
        }
        if self.id is not None:
            output["id"] = self.id
        if self.external_id is not None:
            output["externalId"] = self.external_id

        return output
