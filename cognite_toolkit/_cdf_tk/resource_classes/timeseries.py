from .base import ToolkitResource


class TimeSeriesYAML(ToolkitResource):
    external_id: str
    name: str | None = None
    is_string: bool = False
    metadata: dict[str, str] | None = None
    unit: str | None = None
    unit_external_id: str | None = None
    asset_external_id: str | None = None
    is_step: bool = False
    description: str | None = None
    security_categories: list[str] | None = None
    data_set_external_id: str | None = None
