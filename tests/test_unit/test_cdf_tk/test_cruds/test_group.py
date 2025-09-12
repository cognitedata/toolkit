from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import Group, GroupWrite

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.cruds import (
    DataSetsCRUD,
    ExtractionPipelineCRUD,
    GroupAllScopedCRUD,
    GroupCRUD,
    GroupResourceScopedCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceCRUD,
    ResourceWorker,
    SpaceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestGroupLoader:
    def test_load_all_scoped_only(self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch):
        loader = GroupAllScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
        assert loaded.name == "unscoped_group_name"

        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        with pytest.raises(ToolkitWrongResourceError):
            loader.load_resource(raw_list[0], is_dry_run=False)

    def test_load_resource_scoped_only(self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch):
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        with pytest.raises(ToolkitWrongResourceError):
            raw_list = loader.load_resource_file(
                LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", env_vars_with_client.dump()
            )
            loader.load_resource(raw_list[0], is_dry_run=False)

        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
        assert loaded.name == "scoped_group_name"
        assert len(loaded.capabilities) == 4

        caps = {str(type(element).__name__): element for element in loaded.capabilities}

        assert all(isinstance(item, int) for item in caps["DataSetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["AssetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["ExtractionConfigsAcl"].scope.ids)
        assert caps["SessionsAcl"].scope._scope_name == "all"

    def test_load_group_list_resource_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, GroupWrite)
        assert loaded.name == "scoped_group_name"

    def test_load_group_list_all_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupAllScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[1], is_dry_run=False)

        assert isinstance(loaded, GroupWrite)
        assert loaded.name == "unscoped_group_name"

    def test_unchanged_new_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        # Simulate that one group is is already in CDF
        toolkit_client_approval.append(
            Group,
            [
                Group(
                    id=123,
                    name=loaded.name,
                    source_id=loaded.source_id,
                    capabilities=loaded.capabilities,
                    metadata=loaded.metadata,
                    is_deleted=False,
                )
            ],
        )

        new_file = MagicMock(spec=Path)
        new_file.read_text.return_value = GroupWrite(
            name="new_group", source_id="123", capabilities=loaded.capabilities
        ).dump_yaml()
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources(
            [
                LOAD_DATA / "auth" / "1.my_group_scoped.yaml",
                new_file,
            ]
        )
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 1, "change": 0, "delete": 0, "unchanged": 1}

    def test_upsert_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ):
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(LOAD_DATA / "auth" / "1.my_group_scoped.yaml", env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        # Simulate that the group is is already in CDF, but with fewer capabilities
        # Simulate that one group is is already in CDF
        toolkit_client_approval.append(
            Group,
            [
                Group(
                    id=123,
                    name=loaded.name,
                    source_id=loaded.source_id,
                    capabilities=loaded.capabilities[0:1],
                    metadata=loaded.metadata,
                    is_deleted=False,
                )
            ],
        )

        # group exists, no changes
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([LOAD_DATA / "auth" / "1.my_group_scoped.yaml"])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 1, "delete": 0, "unchanged": 0}

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {"capabilities": [{"dataModelsAcl": {"scope": {"spaceIdScope": {"spaceIds": ["space1", "space2"]}}}}]},
                [(SpaceCRUD, "space1"), (SpaceCRUD, "space2")],
                id="SpaceId scope",
            ),
            pytest.param(
                {"capabilities": [{"timeSeriesAcl": {"scope": {"datasetScope": {"ids": ["ds_dataset1"]}}}}]},
                [
                    (DataSetsCRUD, "ds_dataset1"),
                ],
                id="Dataset scope",
            ),
            pytest.param(
                {
                    "capabilities": [
                        {"extractionRunsAcl": {"scope": {"extractionPipelineScope": {"ids": ["ex_my_extraction"]}}}}
                    ]
                },
                [
                    (ExtractionPipelineCRUD, "ex_my_extraction"),
                ],
                id="Extraction pipeline scope",
            ),
            pytest.param(
                {"capabilities": [{"rawAcl": {"scope": {"tableScope": {"dbsToTables": {"my_db": ["my_table"]}}}}}]},
                [
                    (RawDatabaseCRUD, RawDatabase("my_db")),
                    (RawTableCRUD, RawTable("my_db", "my_table")),
                ],
                id="Table scope",
            ),
            pytest.param(
                {"capabilities": [{"datasetsAcl": {"scope": {"idscope": {"ids": ["ds_my_dataset"]}}}}]},
                [
                    (DataSetsCRUD, "ds_my_dataset"),
                ],
                id="ID scope dataset",
            ),
            pytest.param(
                {"capabilities": [{"extractionPipelinesAcl": {"scope": {"idscope": {"ids": ["ex_my_extraction"]}}}}]},
                [
                    (ExtractionPipelineCRUD, "ex_my_extraction"),
                ],
                id="ID scope extractionpipline ",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual_dependent_items = GroupCRUD.get_dependent_items(item)

        assert list(actual_dependent_items) == expected

    def test_unchanged_new_group_without_metadata(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupAllScopedCRUD.create_loader(env_vars_with_client.get_client())
        local_group = """name: gp_no_metadata
sourceId: 123
capabilities:
- assetsAcl:
    actions:
    - READ
    scope:
      all: {}
"""
        cdf_group = Group.load("""name: gp_no_metadata
sourceId: 123
capabilities:
- assetsAcl:
    actions:
    - READ
    scope:
      all: {}
metadata: {}
id: 3760258445038144
isDeleted: false
deletedTime: -1
""")

        # Simulate that one group is is already in CDF
        toolkit_client_approval.append(
            Group,
            [cdf_group],
        )
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    def test_unchanged_group_raw_acl_table_scoped(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        local_group = """name: gp_raw_acl_table_scoped
sourceId: '123'
capabilities:
- rawAcl:
   actions:
   - READ
   scope:
     tableScope:
       dbsToTables:
         'db_name':
           - labels
        """
        cdf_group = Group.load("""name: gp_raw_acl_table_scoped
sourceId: '123'
capabilities:
- rawAcl:
    actions:
    - READ
    scope:
      tableScope:
        dbsToTables:
          'db_name':
            tables:
            - labels
metadata: {}
id: 3760258445038144
isDeleted: false
deletedTime: -1
        """)

        # Simulate that one group is is already in CDF
        toolkit_client_approval.append(
            Group,
            [cdf_group],
        )
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}
