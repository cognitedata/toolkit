from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client._types import Metadata


class Deduplication(BaseModelObject):
    merge_interval: str | None = None
    activation_interval: str | None = None


class AlertRules(BaseModelObject):
    deduplication: Deduplication | None = None
    exclude_sender: JsonValue | None = None


class AlertChannelResponse(BaseModelObject):
    id: int
    parent_id: int | None = None
    external_id: str | None = None
    parent_external_id: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    alert_rules: AlertRules | None = None
