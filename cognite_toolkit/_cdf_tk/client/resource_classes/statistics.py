"""Resource classes for the Cognite Statistics API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Statistics
"""

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class CountLimit(BaseModelObject):
    """Current count and associated limit for a resource in a project."""

    count: int
    limit: int


class InstanceStatistics(BaseModelObject):
    """Statistics and limits for the number of instances in a project."""

    edges: int
    soft_deleted_edges: int
    nodes: int
    soft_deleted_nodes: int
    instances: int
    instances_limit: int
    soft_deleted_instances: int
    soft_deleted_instances_limit: int


class ProjectStatisticsResponse(BaseModelObject):
    """Statistics and limits for data modeling resources in a project."""

    spaces: CountLimit
    containers: CountLimit
    views: CountLimit
    data_models: CountLimit
    container_properties: CountLimit
    instances: InstanceStatistics
    concurrent_read_limit: int
    concurrent_write_limit: int
    concurrent_delete_limit: int
    records_only_containers: CountLimit | None = None
    records_only_container_properties: CountLimit | None = None


class SpaceStatisticsResponse(BaseModelObject):
    """Data modeling statistics for a single space."""

    space: str
    containers: int
    views: int
    data_models: int
    edges: int
    soft_deleted_edges: int
    nodes: int
    soft_deleted_nodes: int
    container_properties: int | None = None
    records_only_containers: int | None = None
    records_only_container_properties: int | None = None

    @property
    def total_containers(self) -> int:
        """Containers used for nodes/edges plus records-only containers, which `containers` does not count."""
        return self.containers + (self.records_only_containers or 0)

    @property
    def is_empty(self) -> bool:
        """Check if the space has no containers, views, or other resources."""
        return (
            self.containers == 0
            and self.views == 0
            and self.data_models == 0
            and self.edges == 0
            and self.nodes == 0
            and (self.records_only_containers or 0) == 0
        )
