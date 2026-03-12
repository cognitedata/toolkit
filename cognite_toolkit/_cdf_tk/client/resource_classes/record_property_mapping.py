from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId


class RecordPropertyMapping(BaseModelObject):
    """Mapping from asset-centric resource properties to a data modeling container for records migration."""

    external_id: str
    resource_type: str
    container_id: ContainerId
    stream_external_id: str
    property_mapping: dict[str, str]
