from abc import ABC, abstractmethod
from typing import Literal

from cognite_toolkit._cdf_tk.resource_classes.views import ViewReference

from ._base import DataSelector


class InstanceSelector(DataSelector, ABC):
    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


class InstanceViewSelector(InstanceSelector):
    type = "instanceView"
    view: ViewReference
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space]

    def get_instance_spaces(self) -> list[str] | None:
        return list(self.instance_spaces) if self.instance_spaces else None

    @property
    def group(self) -> str:
        return self.view.space

    def __str__(self) -> str:
        return f"{self.view.external_id}_{self.view.version}_{self.instance_type}"
