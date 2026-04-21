from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ViewId


class ContainerInspection(BaseModelObject):
    """Inspection results for a single container."""

    involved_view_count: int = 0
    involved_views: list[ViewId] = []


class InspectedContainer(BaseModelObject):
    """A container together with the results of running container inspection operations on it."""

    space: str
    external_id: str
    inspection_results: ContainerInspection
