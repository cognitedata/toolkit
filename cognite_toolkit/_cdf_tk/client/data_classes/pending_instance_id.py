from dataclasses import dataclass
from typing import Any, Self

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import NodeId


@dataclass
class PendingIdentifier(CogniteObject):
    pending_instance_id: NodeId
    id: int | None = None
    external_id: str | None = None

    def __post_init__(self) -> None:
        if id is None and self.external_id is None:
            raise ValueError("Either 'id' or 'external_id' must be provided for PendingIdentifier.")

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "PendingIdentifier":
        """Load a PendingIdentifier from a resource dictionary."""
        return cls(
            id=resource.get("id"),
            pending_instance_id=NodeId.load(resource["pendingInstanceId"]),
            external_id=resource.get("externalId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the PendingIdentifier to a dictionary."""
        result = super().dump(camel_case)
        result["pendingInstanceId" if camel_case else "pending_instance_id"] = self.pending_instance_id.dump(
            camel_case, include_instance_type=False
        )
        return result


@dataclass(frozen=True)
class UpgradeID:
    id: int | None = None
    external_id: str | None = None
    instance_id: NodeId | None = None

    def dump(self, camel_case: bool = True) -> dict[str, str | dict[str, str]]:
        output: dict[str, str | dict[str, str]] = {}
        if self.id is not None:
            output["id"] = str(self.id)
        if self.external_id is not None:
            output["externalId" if camel_case else "external_id"] = self.external_id
        if self.instance_id is not None:
            output["instanceId" if camel_case else "instance_id"] = self.instance_id.dump(
                camel_case, include_instance_type=False
            )
        return output

    @classmethod
    def load(cls, data: dict[str, Any] | Self) -> Self:
        if isinstance(data, cls):
            return data
        elif isinstance(data, dict):
            return cls(
                id=data.get("id"),
                external_id=data.get("externalId"),
                instance_id=NodeId.load(data["instanceId"]) if "instanceId" in data else None,
            )
        else:
            raise TypeError(f"Expected dict or UpgradeID, got {type(data).__name__}")
