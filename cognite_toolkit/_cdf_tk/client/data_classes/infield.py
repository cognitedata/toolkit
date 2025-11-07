from pydantic import JsonValue

from .base import BaseModelObject, RequestResource, ResponseResource


class DataExplorationConfig(BaseModelObject):
    observations: dict[str, JsonValue]
    activities: dict[str, JsonValue]
    documents: dict[str, JsonValue]
    notifications: dict[str, JsonValue]
    assets: dict[str, JsonValue]


class InfieldLocationConfig(ResponseResource["InfieldLocationConfig"], RequestResource):
    external_id: str

    root_location_external_id: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    app_instance_space: str | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_exploration_config: DataExplorationConfig | None = None

    def as_request_resource(self) -> "InfieldLocationConfig":
        return self
