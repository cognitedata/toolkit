from cognite_toolkit._cdf_tk.client import ToolkitClient
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
            metadata={},
            source_id="",
        )
        # Ideally, this test should have created, and deleted the group for each run. However,
        # creating and deleting groups accumulates in CDF and will eventually cause issues with too many groups, which
        # needs manual cleanup. Therefore, we check that at least once we have be able to create a group
        # with the specified table scope.
        existing_groups = toolkit_client.tool.groups.list(all_groups=True)
        if existing := next((group for group in existing_groups if group.name == group_request.name), None):
            created_group = existing
        else:
            created_group = toolkit_client.tool.groups.create([group_request])

        assert created_group.as_request_resource().dump() == group_request.dump()
