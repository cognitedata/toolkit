from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import NodeId


@dataclass
class PendingIdentifier(CogniteObject):
    id: int
    pending_instance_id: NodeId
    external_id: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "PendingIdentifier":
        """Load a PendingIdentifier from a resource dictionary."""
        return cls(
            id=resource["id"],
            pending_instance_id=NodeId.load(resource["pendingInstanceId"]),
            external_id=resource.get("externalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the PendingIdentifier to a dictionary."""
        result = super().dump(camel_case)
        result["pendingInstanceId"] = self.pending_instance_id.dump(camel_case, include_instance_type=False)
        return result
