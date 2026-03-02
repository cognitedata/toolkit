from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupCapability, GroupRequest, RawAcl, TableScope


class TestGroupsAPI:
    def test_deploy_group_with_table_scope(self, toolkit_client: ToolkitClient, aggregator_raw_db: str) -> None:
        client = toolkit_client
        some_tables = client.tool.raw.tables.list(
            db_name=aggregator_raw_db
        )  # Ensure the database exists before creating the group

        group_request = GroupRequest(
            name="test_group_with_table_scope",
            capabilities=[
                GroupCapability(
                    acl=RawAcl(
                        actions=["READ"],
                        scope=TableScope(dbs_to_tables={aggregator_raw_db: [table.name for table in some_tables]}),
                    ),
                )
            ],
        )
        try:
            created_group = toolkit_client.tool.groups.create([group_request])
        except ToolkitAPIError as e:
            if e.code == 400 and e.duplicated:
                # If the group already exists, retrieve it instead of failing the test
                existing_groups = toolkit_client.tool.groups.list(name=group_request.name)
                assert existing_groups, f"Group with name {group_request.name} should exist but was not found."
                created_group = existing_groups[0]

        assert created_group, "Failed to create or retrieve the group"
