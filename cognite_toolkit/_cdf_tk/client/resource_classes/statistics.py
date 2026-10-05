"""Resource classes for the Cognite Statistics API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Statistics/operation/getStatistics
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


class ProjectStatistics(BaseModelObject):
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
