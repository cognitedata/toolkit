from pydantic import BaseModel, ConfigDict

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str


class ReadModule(BaseModel):
    resources_by_type: dict[ResourceType, list[ToolkitResource]]

    @property
    def is_success(self) -> bool:
        return True
