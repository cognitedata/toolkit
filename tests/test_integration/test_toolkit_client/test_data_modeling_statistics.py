import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.statistics import ProjectStatsAndLimits


@pytest.fixture(scope="session")
def project_usage(toolkit_client: ToolkitClient) -> ProjectStatsAndLimits:
    """Fixture to retrieve project usage statistics."""
    return toolkit_client.data_modeling.statistics.project()


class TestStatisticsAPI:
    def test_list_project_instance_usage(self, project_usage: ProjectStatsAndLimits) -> None:
        assert project_usage.instances.instances < project_usage.instances.instances_limit

    def test_list_space_instance_usage(self, toolkit_client: ToolkitClient) -> None:
        spaces = toolkit_client.data_modeling.spaces.list(limit=1)
        assert len(spaces) > 0
        selected_space = spaces[0].space

        space_usage = toolkit_client.data_modeling.statistics.list(space=selected_space)

        assert space_usage.space == selected_space

    def test_list_all_spaces(self, toolkit_client: ToolkitClient, project_usage: ProjectStatsAndLimits) -> None:
        space_usages = toolkit_client.data_modeling.statistics.list()

        assert len(space_usages) > 0
        total = sum(space_usage.nodes + space_usage.edges for space_usage in space_usages)

        assert total == project_usage.instances.instances
