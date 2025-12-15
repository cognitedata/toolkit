import sys
from dataclasses import dataclass
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ProjectStatus(CogniteResource):
    """Project status information."""

    data_modeling_status: Literal["HYBRID", "DATA_MODELING_ONLY"]
    url_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            data_modeling_status=resource["dataModelingStatus"],
            url_name=resource["urlName"],
        )


class ProjectStatusList(CogniteResourceList[ProjectStatus]):
    """List of ProjectStatus items."""

    _RESOURCE = ProjectStatus

    @property
    def this_project(self) -> ProjectStatus:
        """Returns the ProjectStatus of the current project."""
        project = self._cognite_client.config.project
        for item in self:
            if item.url_name == project:
                return item
        raise ValueError(f"Project '{project}' not found in the list of projects.")
