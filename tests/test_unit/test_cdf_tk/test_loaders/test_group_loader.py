from collections.abc import Hashable

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import Group, GroupWrite

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import (
    DataSetsLoader,
    ExtractionPipelineLoader,
    GroupAllScopedLoader,
    GroupLoader,
    GroupResourceScopedLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
    SpaceLoader,
)
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestGroupLoader:
    def test_load_all_scoped_only(self, cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = GroupAllScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", cdf_tool_config, skip_validation=False
        )
        assert loaded.name == "unscoped_group_name"

        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=False
        )
        assert loaded is None

    def test_load_resource_scoped_only(self, cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_unscoped.yaml", cdf_tool_config, skip_validation=False
        )

        assert loaded is None

        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=False
        )
        assert loaded.name == "scoped_group_name"
        assert len(loaded.capabilities) == 4

        caps = {str(type(element).__name__): element for element in loaded.capabilities}

        assert all(isinstance(item, int) for item in caps["DataSetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["AssetsAcl"].scope.ids)
        assert all(isinstance(item, int) for item in caps["ExtractionConfigsAcl"].scope.ids)
        assert caps["SessionsAcl"].scope._scope_name == "all"

    def test_load_group_list_resource_scoped_only(self, cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", cdf_tool_config, skip_validation=True
        )

        assert isinstance(loaded, GroupWrite)
        assert loaded.name == "scoped_group_name"

    def test_load_group_list_all_scoped_only(self, cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = GroupAllScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_list_combined.yaml", cdf_tool_config, skip_validation=True
        )

        assert isinstance(loaded, GroupWrite)
        assert loaded.name == "unscoped_group_name"

    def test_unchanged_new_group(
        self, cdf_tool_config: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=True
        )

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

        new_group = GroupWrite(name="new_group", source_id="123", capabilities=[])
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(
            resources=[loaded, new_group], loader=loader
        )

        assert len(to_create) == 1
        assert len(to_change) == 0
        assert len(unchanged) == 1

    def test_upsert_group(
        self, cdf_tool_config: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=True
        )
        cmd = DeployCommand(print_warning=False)

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
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(resources=[loaded], loader=loader)

        assert len(to_create) == 0
        assert len(to_change) == 1
        assert len(unchanged) == 0

        cmd._update_resources(
            to_change,
            loader,
        )

        assert toolkit_client_approval.create_calls()["Group"] == 1
        assert toolkit_client_approval.delete_calls()["Group"] == 1

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {"capabilities": [{"dataModelsAcl": {"scope": {"spaceIdScope": {"spaceIds": ["space1", "space2"]}}}}]},
                [(SpaceLoader, "space1"), (SpaceLoader, "space2")],
                id="SpaceId scope",
            ),
            pytest.param(
                {"capabilities": [{"timeSeriesAcl": {"scope": {"datasetScope": {"ids": ["ds_dataset1"]}}}}]},
                [
                    (DataSetsLoader, "ds_dataset1"),
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
                    (ExtractionPipelineLoader, "ex_my_extraction"),
                ],
                id="Extraction pipeline scope",
            ),
            pytest.param(
                {"capabilities": [{"rawAcl": {"scope": {"tableScope": {"dbsToTables": {"my_db": ["my_table"]}}}}}]},
                [
                    (RawDatabaseLoader, RawDatabaseTable("my_db")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table")),
                ],
                id="Table scope",
            ),
            pytest.param(
                {"capabilities": [{"datasetsAcl": {"scope": {"idscope": {"ids": ["ds_my_dataset"]}}}}]},
                [
                    (DataSetsLoader, "ds_my_dataset"),
                ],
                id="ID scope dataset",
            ),
            pytest.param(
                {"capabilities": [{"extractionPipelinesAcl": {"scope": {"idscope": {"ids": ["ex_my_extraction"]}}}}]},
                [
                    (ExtractionPipelineLoader, "ex_my_extraction"),
                ],
                id="ID scope extractionpipline ",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual_dependent_items = GroupLoader.get_dependent_items(item)

        assert list(actual_dependent_items) == expected
