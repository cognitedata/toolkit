from collections.abc import Hashable
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    AssetsAcl,
    GroupCapability,
    GroupRequest,
    GroupResponse,
)
from cognite_toolkit._cdf_tk.commands import DeployV2Command
from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError
from cognite_toolkit._cdf_tk.resource_ios import (
    DataProductIO,
    DataSetsIO,
    ExtractionPipelineIO,
    GroupAllScopedCRUD,
    GroupIO,
    GroupResourceScopedCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceIO,
    SpaceCRUD,
)
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

        caps = {type(cap.acl).__name__: cap.acl for cap in loaded.capabilities}

        assert all(isinstance(item, int) for item in caps["DataSetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["AssetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["ExtractionConfigsAcl"].scope.ids)
        assert caps["SessionsAcl"].scope.scope_name == "all"

    def test_load_group_list_resource_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, GroupRequest)
        assert loaded.name == "scoped_group_name"

    def test_load_group_list_all_scoped_only(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ):
        loader = GroupAllScopedCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[1], is_dry_run=False)

        assert isinstance(loaded, GroupRequest)
        assert loaded.name == "unscoped_group_name"

    def test_unchanged_new_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        filepath = LOAD_DATA / "auth" / "1.my_group_scoped.yaml"
        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(deepcopy(raw_list[0]), is_dry_run=False)

        # Simulate that one group is already in CDF
        cdf_group = GroupResponse(
            id=123,
            name=loaded.name,
            source_id=loaded.source_id,
            capabilities=loaded.capabilities,
            metadata=loaded.metadata,
            is_deleted=False,
        )
        toolkit_client_approval.append(GroupResponse, [cdf_group])

        new_group = GroupRequest(name="new_group", source_id="123", capabilities=loaded.capabilities)
        new_file = MagicMock(spec=Path)
        new_file.read_text.return_value = new_group.dump_yaml()
        loaded_id = loader.get_id(loaded)
        new_raw = loader.load_resource_file(new_file, env_vars_with_client.dump())
        new_loaded = loader.load_resource(deepcopy(new_raw[0]), is_dry_run=False)
        new_id = loader.get_id(new_loaded)
        existing_list = loader.retrieve([loaded_id, new_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={
                loaded_id: ReadResource(loaded, raw_list[0], [filepath]),
                new_id: ReadResource(new_loaded, new_raw[0], [new_file]),
            },
            cdf_by_id={loader.get_id(item): item for item in existing_list},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 1, "change": 0, "delete": 0, "unchanged": 1}

    def test_upsert_group(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ):
        loader = GroupResourceScopedCRUD.create_loader(env_vars_with_client.get_client())
        filepath = LOAD_DATA / "auth" / "1.my_group_scoped.yaml"
        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(deepcopy(raw_list[0]), is_dry_run=False)

        # Simulate that the group is already in CDF, but with fewer capabilities
        cdf_group = GroupResponse(
            id=123,
            name=loaded.name,
            source_id=loaded.source_id,
            capabilities=loaded.capabilities[0:1],
            metadata=loaded.metadata,
            is_deleted=False,
        )
        toolkit_client_approval.append(GroupResponse, [cdf_group])

        resource_id = loader.get_id(loaded)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(loaded, raw_list[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )

        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 0, "change": 1, "delete": 0, "unchanged": 0}

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {"capabilities": [{"dataModelsAcl": {"scope": {"spaceIdScope": {"spaceIds": ["space1", "space2"]}}}}]},
                [(SpaceCRUD, SpaceId(space="space1")), (SpaceCRUD, SpaceId(space="space2"))],
                id="SpaceId scope",
            ),
            pytest.param(
                {"capabilities": [{"timeSeriesAcl": {"scope": {"datasetScope": {"ids": ["ds_dataset1"]}}}}]},
                [
                    (DataSetsIO, ExternalId(external_id="ds_dataset1")),
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
                    (ExtractionPipelineIO, ExternalId(external_id="ex_my_extraction")),
                ],
                id="Extraction pipeline scope",
            ),
            pytest.param(
                {"capabilities": [{"rawAcl": {"scope": {"tableScope": {"dbsToTables": {"my_db": ["my_table"]}}}}}]},
                [
                    (RawDatabaseCRUD, RawDatabaseId(name="my_db")),
                    (RawTableCRUD, RawTableId(db_name="my_db", name="my_table")),
                ],
                id="Table scope",
            ),
            pytest.param(
                {"capabilities": [{"datasetsAcl": {"scope": {"idscope": {"ids": ["ds_my_dataset"]}}}}]},
                [
                    (DataSetsIO, ExternalId(external_id="ds_my_dataset")),
                ],
                id="ID scope dataset",
            ),
            pytest.param(
                {"capabilities": [{"extractionPipelinesAcl": {"scope": {"idscope": {"ids": ["ex_my_extraction"]}}}}]},
                [
                    (ExtractionPipelineIO, ExternalId(external_id="ex_my_extraction")),
                ],
                id="ID scope extractionpipline ",
            ),
            pytest.param(
                {
                    "capabilities": [
                        {"dataProductsAcl": {"scope": {"dataProductScope": {"externalIds": ["my-data-product"]}}}}
                    ]
                },
                [
                    (DataProductIO, ExternalId(external_id="my-data-product")),
                ],
                id="Data product scope",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceIO], Hashable]]) -> None:
        actual_dependent_items = GroupIO.get_dependent_items(item)

        assert list(actual_dependent_items) == expected

    def test_unchanged_new_group_without_metadata(
        self,
        env_vars_with_client: EnvironmentVariables,
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = GroupAllScopedCRUD.create_loader(env_vars_with_client.get_client())
        local_group = """name: gp_no_metadata
sourceId: '123'
capabilities:
- assetsAcl:
    actions:
    - READ
    scope:
      all: {}
"""
        cdf_group = GroupResponse(
            name="gp_no_metadata",
            source_id="123",
            capabilities=[GroupCapability(acl=AssetsAcl(actions=["READ"], scope=AllScope()))],
            metadata={},
            id=3760258445038144,
            is_deleted=False,
        )

        # Simulate that one group is already in CDF
        toolkit_client_approval.append(GroupResponse, [cdf_group])
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(deepcopy(resource_dict[0]), is_dry_run=False)
        resource_id = loader.get_id(resource)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
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
        cdf_group = GroupResponse(
            name="gp_raw_acl_table_scoped",
            source_id="123",
            capabilities=[
                {
                    "rawAcl": {
                        "actions": ["READ"],
                        "scope": {
                            "tableScope": {
                                "dbsToTables": {
                                    "db_name": ["labels"],
                                }
                            }
                        },
                    }
                }
            ],
            metadata={},
            id=3760258445038144,
            is_deleted=False,
        )

        # Simulate that one group is already in CDF
        toolkit_client_approval.append(GroupResponse, [cdf_group])
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_group

        resource_dict = loader.load_resource_file(filepath, {})
        assert len(resource_dict) == 1
        resource = loader.load_resource(deepcopy(resource_dict[0]), is_dry_run=False)
        resource_id = loader.get_id(resource)
        existing_list = loader.retrieve([resource_id])
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id={resource_id: ReadResource(resource, resource_dict[0], [filepath])},
            cdf_by_id={resource_id: existing_list[0]},
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}
