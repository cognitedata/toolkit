import sys
from typing import Any

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import RequestItem

from .instance_api import NodeReference

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class PendingInstanceId(RequestItem):
    """A pending instance ID links an asset-centric resource (TimeSeries or FileMetadata)
    to a DM node that will be created later by the syncer service.
    """

    pending_instance_id: NodeReference
    id: int | None = None
    external_id: str | None = None

    @model_validator(mode="after")
    def validate_exactly_one_id(self) -> Self:
        if (self.id is None) == (self.external_id is None):
            raise ValueError("Exactly one of id or external_id must be set.")
        return self

    def __str__(self) -> str:
        id_part = f"id={self.id}" if self.id is not None else f"externalId={self.external_id}"
        return f"PendingInstanceId({id_part})"

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)
