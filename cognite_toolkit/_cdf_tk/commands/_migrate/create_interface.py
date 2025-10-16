from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from collections.abc import Iterable

from cognite.client.data_classes._base import T_CogniteResourceList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import ResourceCRUD

@dataclass
class ResourceConfig:
    filename: str
    data: dict[str, Any]


class MigrationCreate(ABC):
    """Base class for migration resources configurations that are created from asset-centric resources."""
    CRUD: type[ResourceCRUD]
    DISPLAY_NAME: str

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    def prepare(self) -> None:
        raise NotImplementedError("Subclasses should implement this method")

    @abstractmethod
    def deploy_resources(self) -> T_CogniteResourceList:
        raise NotImplementedError("Subclasses should implement this method")

    def resource_configs(self) -> list[ResourceConfig]:
        raise NotImplementedError("Subclasses should implement this method")