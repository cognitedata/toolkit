from pydantic import JsonValue

from .base import BaseModelObject, RequestResource, ResponseResource


class DataExplorationConfig(BaseModelObject):
    observations: dict[str, JsonValue] | None = None
    activities: dict[str, JsonValue] | None = None
    documents: dict[str, JsonValue] | None = None
    notifications: dict[str, JsonValue] | None = None
    assets: dict[str, JsonValue] | None = None


class InfieldLocationConfig(ResponseResource["InfieldLocationConfig"], RequestResource):
    space: str
    external_id: str

    root_location_external_id: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    app_instance_space: str | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_exploration_config: DataExplorationConfig | None = None

    def as_request_resource(self) -> "InfieldLocationConfig":
        return self
