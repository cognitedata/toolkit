import os
import pathlib
from collections import Counter, defaultdict
from collections.abc import Hashable, Iterable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    DataSet,
    ExtractionPipelineConfig,
    FileMetadata,
    FunctionWrite,
    Group,
    GroupWrite,
    Transformation,
    TransformationSchedule,
)
from cognite.client.data_classes.data_modeling import Edge, Node, NodeApply
from pytest import MonkeyPatch
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk._parameters import ParameterSet, read_parameters_from_dict
from cognite_toolkit._cdf_tk.commands import BuildCommand, CleanCommand, DeployCommand
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    Environment,
    InitConfigYAML,
    SystemYAML,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
    ContainerLoader,
    DataModelLoader,
    DatapointsLoader,
    DataSetsLoader,
    ExtractionPipelineConfigLoader,
    ExtractionPipelineLoader,
    FileMetadataLoader,
    FunctionLoader,
    GroupAllScopedLoader,
    GroupLoader,
    GroupResourceScopedLoader,
    Loader,
    NodeLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
    ResourceTypes,
    SpaceLoader,
    TimeSeriesLoader,
    TransformationLoader,
    ViewLoader,
)
from cognite_toolkit._cdf_tk.loaders.data_classes import NodeAPICall, NodeApplyListWithCall, RawDatabaseTable
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    module_from_path,
    resource_folder_from_path,
    tmp_build_directory,
)
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml
from tests.constants import REPO_ROOT
from tests.tests_unit.approval_client import ApprovalCogniteClient
from tests.tests_unit.data import LOAD_DATA, PYTEST_PROJECT
from tests.tests_unit.test_cdf_tk.constants import BUILD_DIR, SNAPSHOTS_DIR_ALL
from tests.tests_unit.utils import FakeCogniteResourceGenerator, mock_read_yaml_file

SNAPSHOTS_DIR = SNAPSHOTS_DIR_ALL / "load_data_snapshots"


@pytest.mark.parametrize(
    "loader_cls",
    [
        FileMetadataLoader,
        DatapointsLoader,
    ],
)
def test_loader_class(
    loader_cls: type[ResourceLoader],
    cognite_client_approval: ApprovalCogniteClient,
    data_regression: DataRegressionFixture,
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.data_set_id = 999

    cmd = DeployCommand(print_warning=False)
    loader = loader_cls.create_loader(cdf_tool, LOAD_DATA)
    cmd.deploy_resources(loader, cdf_tool, dry_run=False)

    dump = cognite_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{loader.folder_name}.yaml")


class TestFunctionLoader:
    def test_load_functions(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

        loader = FunctionLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(LOAD_DATA / "functions" / "1.my_functions.yaml", cdf_tool, skip_validation=False)
        assert len(loaded) == 2

    def test_load_function(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

        loader = FunctionLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(LOAD_DATA / "functions" / "1.my_function.yaml", cdf_tool, skip_validation=False)
        assert isinstance(loaded, FunctionWrite)


class TestDataSetsLoader:
    def test_upsert_data_set(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client

        loader = DataSetsLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(LOAD_DATA / "data_sets" / "1.my_datasets.yaml", cdf_tool, skip_validation=False)
        assert len(loaded) == 2

        first = DataSet.load(loaded[0].dump())
        # Set the properties that are set on the server side
        first.id = 42
        first.created_time = 42
        first.last_updated_time = 42
        # Simulate that the data set is already in CDF
        cognite_client_approval.append(DataSet, first)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(loaded, loader)

        assert len(to_create) == 1
        assert len(to_change) == 0
        assert len(unchanged) == 1


class TestContainerLoader:
    @pytest.mark.parametrize(
        "item",
        [
            pytest.param(
                {
                    "properties": {
                        "myDirectRelation": {
                            "name": "my direct relation",
                            "type": {
                                "type": "direct",
                                "container": {
                                    "type": "container",
                                    "space": "sp_my_space",
                                    "externalId": "my_container",
                                },
                            },
                        }
                    }
                },
                id="Direct relation property with require constraint.",
            ),
        ],
    )
    def test_valid_spec(self, item: dict):
        spec = ContainerLoader.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"


class TestViewLoader:
    @pytest.mark.parametrize(
        "item",
        [
            pytest.param(
                {
                    "filter": {
                        "hasData": [
                            {"type": "container", "space": "sp_my_space", "externalId": "container_id"},
                            {"type": "view", "space": "sp_my_space", "externalId": "view_id"},
                        ]
                    }
                },
                id="HasData Filter",
            ),
            pytest.param(
                {
                    "properties": {
                        "reverseDirectRelation": {
                            "connectionType": "multi_reverse_direct_relation",
                            "source": {
                                "type": "view",
                                "space": "sp_my_space",
                                "externalId": "view_id",
                                "version": "v42",
                            },
                            "through": {
                                "source": {
                                    "type": "view",
                                    "space": "sp_my_space",
                                    "externalId": "view_id",
                                    "version": "v42",
                                },
                                "identifier": "view_property",
                            },
                        }
                    }
                },
                id="Reverse Direct Relation Property",
            ),
        ],
    )
    def test_valid_spec(self, item: dict):
        spec = ViewLoader.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"

    def test_update_view_with_interface(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client
        prop1 = dm.MappedProperty(
            dm.ContainerId(space="sp_space", external_id="container_id"),
            "prop1",
            type=dm.Text(),
            nullable=True,
            auto_increment=False,
        )
        interface = dm.View(
            space="sp_space",
            external_id="interface",
            version="1",
            properties={"prop1": prop1},
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
        )
        # Note that child views always contain all properties of their parent interfaces.
        child_cdf = dm.View(
            space="sp_space",
            external_id="child",
            version="1",
            properties={"prop1": prop1},
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=[interface.as_id()],
            writable=True,
            used_for="node",
            is_global=False,
        )
        child_local = dm.ViewApply(
            space="sp_space",
            external_id="child",
            version="1",
            implements=[interface.as_id()],
        )
        # Simulating that the interface and child_cdf are available in CDF
        cognite_client_approval.append(dm.View, [interface, child_cdf])

        loader = ViewLoader.create_loader(cdf_tool, None)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(
            dm.ViewApplyList([child_local]), loader
        )

        assert len(to_create) == 0
        assert len(to_change) == 0
        assert len(unchanged) == 1

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "name": {
                            "container": {
                                "type": "container",
                                "space": "my_container_space",
                                "externalId": "my_container",
                            }
                        }
                    },
                },
                [
                    (SpaceLoader, "sp_my_space"),
                    (ContainerLoader, dm.ContainerId(space="my_container_space", external_id="my_container")),
                ],
                id="View with one container property",
            ),
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "toEdge": {
                            "source": {
                                "type": "view",
                                "space": "my_view_space",
                                "externalId": "my_view",
                                "version": "1",
                            },
                            "edgeSource": {
                                "type": "view",
                                "space": "my_other_view_space",
                                "externalId": "my_edge_view",
                                "version": "42",
                            },
                        }
                    },
                },
                [
                    (SpaceLoader, "sp_my_space"),
                    (ViewLoader, dm.ViewId(space="my_view_space", external_id="my_view", version="1")),
                    (ViewLoader, dm.ViewId(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = ViewLoader.get_dependent_items(item)

        assert list(actual) == expected


class TestDataModelLoader:
    def test_update_data_model_random_view_order(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client
        cdf_data_model = dm.DataModel(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                dm.ViewId(space="sp_space", external_id="first", version="1"),
                dm.ViewId(space="sp_space", external_id="second", version="1"),
            ],
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            is_global=False,
        )
        # Simulating that the data model is available in CDF
        cognite_client_approval.append(dm.DataModel, cdf_data_model)

        local_data_model = dm.DataModelApply(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                dm.ViewId(space="sp_space", external_id="second", version="1"),
                dm.ViewId(space="sp_space", external_id="first", version="1"),
            ],
            description=None,
            name=None,
        )

        loader = DataModelLoader.create_loader(cdf_tool, None)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(
            dm.DataModelApplyList([local_data_model]), loader
        )

        assert len(to_create) == 0
        assert len(to_change) == 0
        assert len(unchanged) == 1


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
        self, cdf_tool_config: CDFToolConfig, cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=True
        )

        # Simulate that one group is is already in CDF
        cognite_client_approval.append(
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
        self, cdf_tool_config: CDFToolConfig, cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch
    ):
        loader = GroupResourceScopedLoader.create_loader(cdf_tool_config, None)
        loaded = loader.load_resource(
            LOAD_DATA / "auth" / "1.my_group_scoped.yaml", cdf_tool_config, skip_validation=True
        )
        cmd = DeployCommand(print_warning=False)

        # Simulate that the group is is already in CDF, but with fewer capabilities
        # Simulate that one group is is already in CDF
        cognite_client_approval.append(
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

        assert cognite_client_approval.create_calls()["Group"] == 1
        assert cognite_client_approval.delete_calls()["Group"] == 1

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


class TestTimeSeriesLoader:
    timeseries_yaml = """
externalId: pi_160696
name: VAL_23-PT-92504:X.Value
dataSetExternalId: ds_timeseries_oid
isString: false
metadata:
  compdev: '0'
  location5: '2'
isStep: false
description: PH 1stStgSuctCool Gas Out
"""

    def test_load_skip_validation_no_preexisting_dataset(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TimeSeriesLoader(cognite_client_approval.mock_client, None)
        mock_read_yaml_file({"timeseries.yaml": yaml.safe_load(self.timeseries_yaml)}, monkeypatch)
        loaded = loader.load_resource(Path("timeseries.yaml"), cdf_tool_config_real, skip_validation=True)

        assert len(loaded) == 1
        assert loaded[0].data_set_id == -1

    def test_load_skip_validation_with_preexisting_dataset(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        cognite_client_approval.append(DataSet, DataSet(id=12345, external_id="ds_timeseries_oid"))
        loader = TimeSeriesLoader(cognite_client_approval.mock_client, None)

        mock_read_yaml_file({"timeseries.yaml": yaml.safe_load(self.timeseries_yaml)}, monkeypatch)

        loaded = loader.load_resource(Path("timeseries.yaml"), cdf_tool_config_real, skip_validation=True)

        assert len(loaded) == 1
        assert loaded[0].data_set_id == 12345


class TestTransformationLoader:
    trafo_yaml = """
externalId: tr_first_transformation
name: 'example:first:transformation'
interval: '{{scheduleHourly}}'
isPaused: true
query: "INLINE"
destination:
  type: 'assets'
ignoreNullFields: true
isPublic: true
conflictMode: upsert
"""

    trafo_sql = "FILE"

    def test_no_auth_load(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)
        mock_read_yaml_file({"transformation.yaml": yaml.CSafeLoader(self.trafo_yaml).get_data()}, monkeypatch)
        loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
        assert loaded.destination_oidc_credentials is None
        assert loaded.source_oidc_credentials is None

    def test_oidc_auth_load(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        resource["authentication"] = {
            "clientId": "{{cicd_clientId}}",
            "clientSecret": "{{cicd_clientSecret}}",
            "tokenUri": "{{cicd_tokenUri}}",
            "cdfProjectName": "{{cdfProjectName}}",
            "scopes": "{{cicd_scopes}}",
            "audience": "{{cicd_audience}}",
        }

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
        assert loaded.destination_oidc_credentials.dump() == loaded.source_oidc_credentials.dump()
        assert loaded.destination is not None

    def test_oidc_raise_if_invalid(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        resource["authentication"] = {
            "clientId": "{{cicd_clientId}}",
            "clientSecret": "{{cicd_clientSecret}}",
        }

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with pytest.raises(ToolkitYAMLFormatError):
            loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)

    def test_sql_file(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()
        resource.pop("query")
        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with patch.object(TransformationLoader, "_get_query_file", return_value=Path("transformation.sql")):
            with patch.object(pathlib.Path, "read_text", return_value=self.trafo_sql):
                loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
                assert loaded.query == self.trafo_sql

    def test_sql_inline(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with patch.object(TransformationLoader, "_get_query_file", return_value=None):
            loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
            assert loaded.query == resource["query"]

    def test_if_ambiguous(
        self,
        cognite_client_approval: ApprovalCogniteClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(cognite_client_approval.mock_client, None)

        mock_read_yaml_file({"transformation.yaml": yaml.CSafeLoader(self.trafo_yaml).get_data()}, monkeypatch)

        with pytest.raises(ToolkitYAMLFormatError):
            with patch.object(TransformationLoader, "_get_query_file", return_value=Path("transformation.sql")):
                with patch.object(pathlib.Path, "read_text", return_value=self.trafo_sql):
                    loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "dataSetExternalId": "ds_my_dataset",
                    "destination": {
                        "type": "instances",
                        "dataModel": {
                            "space": "sp_model_space",
                            "externalId": "my_model",
                            "version": "v1",
                            "destinationType": "assets",
                        },
                        "instanceSpace": "sp_data_space",
                    },
                },
                [
                    (DataSetsLoader, "ds_my_dataset"),
                    (SpaceLoader, "sp_data_space"),
                    (DataModelLoader, dm.DataModelId(space="sp_model_space", external_id="my_model", version="v1")),
                ],
                id="Transformation to data model",
            ),
            pytest.param(
                {
                    "destination": {
                        "type": "nodes",
                        "view": {"space": "sp_space", "externalId": "my_view", "version": "v1"},
                        "instanceSpace": "sp_data_space",
                    }
                },
                [
                    (SpaceLoader, "sp_data_space"),
                    (ViewLoader, dm.ViewId(space="sp_space", external_id="my_view", version="v1")),
                ],
                id="Transformation to nodes ",
            ),
            pytest.param(
                {"destination": {"type": "raw", "database": "my_db", "table": "my_table"}},
                [
                    (RawDatabaseLoader, RawDatabaseTable("my_db")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table")),
                ],
                id="Transformation to RAW table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = TransformationLoader.get_dependent_items(item)

        assert list(actual) == expected


class TestNodeLoader:
    @pytest.mark.parametrize(
        "yamL_raw, expected",
        [
            pytest.param(
                """space: my_space
externalId: my_external_id""",
                NodeApplyListWithCall([NodeApply("my_space", "my_external_id")]),
                id="Single node no API call",
            ),
            pytest.param(
                """- space: my_space
  externalId: my_first_node
- space: my_space
  externalId: my_second_node
""",
                NodeApplyListWithCall(
                    [
                        NodeApply("my_space", "my_first_node"),
                        NodeApply("my_space", "my_second_node"),
                    ]
                ),
                id="Multiple nodes no API call",
            ),
            pytest.param(
                """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
node:
  space: my_space
  externalId: my_external_id""",
                NodeApplyListWithCall([NodeApply("my_space", "my_external_id")], NodeAPICall(True, False, True)),
                id="Single node with API call",
            ),
            pytest.param(
                """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
nodes:
- space: my_space
  externalId: my_first_node
- space: my_space
  externalId: my_second_node
    """,
                NodeApplyListWithCall(
                    [
                        NodeApply("my_space", "my_first_node"),
                        NodeApply("my_space", "my_second_node"),
                    ],
                    NodeAPICall(True, False, True),
                ),
                id="Multiple nodes with API call",
            ),
        ],
    )
    def test_load_nodes(
        self,
        yamL_raw: str,
        expected: NodeApplyListWithCall,
        cdf_tool_config: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = NodeLoader.create_loader(cdf_tool_config, None)
        mock_read_yaml_file({"my_node.yaml": yaml.safe_load(yamL_raw)}, monkeypatch)
        loaded = loader.load_resource(Path("my_node.yaml"), cdf_tool_config, skip_validation=True)

        assert loaded.dump() == expected.dump()


class TestExtractionPipelineDependencies:
    _yaml = """
        externalId: 'ep_src_asset_hamburg_sap'
        name: 'Hamburg SAP'
        dataSetId: 12345
    """

    config_yaml = """
        externalId: 'ep_src_asset'
        description: 'DB extractor config reading data from Springfield SAP'
    """

    def test_load_extraction_pipeline_upsert_create_one(
        self, cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch
    ):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client

        cognite_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
            ),
        )

    def test_load_extraction_pipeline_upsert_update_one(
        self, cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch
    ):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client

        cognite_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
                config="\n    logger: \n        {level: WARN}",
            ),
        )

        mock_read_yaml_file(
            {"extraction_pipeline.config.yaml": yaml.CSafeLoader(self.config_yaml).get_data()}, monkeypatch
        )

        cmd = DeployCommand(print_warning=False)
        loader = ExtractionPipelineConfigLoader.create_loader(cdf_tool, None)
        resources = loader.load_resource(Path("extraction_pipeline.config.yaml"), cdf_tool, skip_validation=False)
        to_create, changed, unchanged = cmd.to_create_changed_unchanged_triple([resources], loader)
        assert len(to_create) == 0
        assert len(changed) == 1
        assert len(unchanged) == 0

    def test_load_extraction_pipeline_delete_one(
        self, cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch
    ):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client

        cognite_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
                config="\n    logger: \n        {level: WARN}",
            ),
        )

        mock_read_yaml_file(
            {"extraction_pipeline.config.yaml": yaml.CSafeLoader(self.config_yaml).get_data()}, monkeypatch
        )

        cmd = CleanCommand(print_warning=False)
        loader = ExtractionPipelineConfigLoader.create_loader(cdf_tool, None)
        with patch.object(
            ExtractionPipelineConfigLoader, "find_files", return_value=[Path("extraction_pipeline.config.yaml")]
        ):
            res = cmd.clean_resources(loader, cdf_tool, dry_run=True, drop=True)
            assert res.deleted == 1


class TestExtractionPipelineLoader:
    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "dataSetExternalId": "ds_my_dataset",
                    "rawTables": [
                        {"dbName": "my_db", "tableName": "my_table"},
                        {"dbName": "my_db", "tableName": "my_table2"},
                    ],
                },
                [
                    (DataSetsLoader, "ds_my_dataset"),
                    (RawDatabaseLoader, RawDatabaseTable("my_db")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table2")),
                ],
                id="Extraction pipeline to Table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = ExtractionPipelineLoader.get_dependent_items(item)

        assert list(actual) == expected


class TestDeployResources:
    def test_deploy_resource_order(self, cognite_client_approval: ApprovalCogniteClient):
        build_env_name = "dev"
        system_config = SystemYAML.load_from_directory(PYTEST_PROJECT, build_env_name)
        config = BuildConfigYAML.load_from_directory(PYTEST_PROJECT, build_env_name)
        config.environment.selected = ["another_module"]
        build_cmd = BuildCommand()
        build_cmd.build_config(
            BUILD_DIR, PYTEST_PROJECT, config=config, system_config=system_config, clean=True, verbose=False
        )
        expected_order = ["MyView", "MyOtherView"]
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client

        cmd = DeployCommand(print_warning=False)
        cmd.deploy_resources(ViewLoader.create_loader(cdf_tool, BUILD_DIR), cdf_tool, dry_run=False)

        views = cognite_client_approval.dump(sort=False)["View"]

        actual_order = [view["externalId"] for view in views]

        assert actual_order == expected_order


class TestFormatConsistency:
    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_fake_resource_generator(
        self, Loader: type[ResourceLoader], cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch
    ):
        fakegenerator = FakeCogniteResourceGenerator(seed=1337)

        loader = Loader.create_loader(cdf_tool_config, None)
        instance = fakegenerator.create_instance(loader.resource_write_cls)

        assert isinstance(instance, loader.resource_write_cls)

    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_loader_takes_dict(
        self, Loader: type[ResourceLoader], cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch
    ):
        loader = Loader.create_loader(cdf_tool_config, None)

        if loader.resource_cls in [Transformation, FileMetadata]:
            pytest.skip("Skipped loaders that require secondary files")
        elif loader.resource_cls in [Edge, Node]:
            pytest.skip(f"Skipping {loader.resource_cls} because it has special properties")
        elif Loader in [GroupResourceScopedLoader]:
            pytest.skip(f"Skipping {loader.resource_cls} because it requires scoped capabilities")

        instance = FakeCogniteResourceGenerator(seed=1337).create_instance(loader.resource_write_cls)

        # special case
        if isinstance(instance, TransformationSchedule):
            del instance.id  # Client validation does not allow id and externalid to be set simultaneously

        mock_read_yaml_file({"dict.yaml": instance.dump()}, monkeypatch)

        loaded = loader.load_resource(
            filepath=Path(loader.folder_name) / "dict.yaml", ToolGlobals=cdf_tool_config, skip_validation=True
        )
        assert isinstance(
            loaded, (loader.resource_write_cls, loader.list_write_cls)
        ), f"loaded must be an instance of {loader.list_write_cls} or {loader.resource_write_cls} but is {type(loaded)}"

    @pytest.mark.parametrize("Loader", RESOURCE_LOADER_LIST)
    def test_loader_takes_list(
        self, Loader: type[ResourceLoader], cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch
    ):
        loader = Loader.create_loader(cdf_tool_config, None)

        if loader.resource_cls in [Transformation, FileMetadata]:
            pytest.skip("Skipped loaders that require secondary files")
        elif loader.resource_cls in [Edge, Node]:
            pytest.skip(f"Skipping {loader.resource_cls} because it has special properties")
        elif Loader in [GroupResourceScopedLoader]:
            pytest.skip(f"Skipping {loader.resource_cls} because it requires scoped capabilities")

        instances = FakeCogniteResourceGenerator(seed=1337).create_instances(loader.list_write_cls)

        # special case
        if isinstance(loader.resource_cls, TransformationSchedule):
            for instance in instances:
                del instance.id  # Client validation does not allow id and externalid to be set simultaneously

        mock_read_yaml_file({"dict.yaml": instances.dump()}, monkeypatch)

        loaded = loader.load_resource(
            filepath=Path(loader.folder_name) / "dict.yaml", ToolGlobals=cdf_tool_config, skip_validation=True
        )
        assert isinstance(
            loaded, (loader.resource_write_cls, loader.list_write_cls)
        ), f"loaded must be an instance of {loader.list_write_cls} or {loader.resource_write_cls} but is {type(loaded)}"

    @staticmethod
    def check_url(url) -> bool:
        try:
            response = requests.get(url, allow_redirects=True)
            return response.status_code >= 200 and response.status_code <= 300
        except requests.exceptions.RequestException:
            return False

    @pytest.mark.parametrize("Loader", LOADER_LIST)
    def test_loader_has_doc_url(self, Loader: type[Loader], cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = Loader.create_loader(cdf_tool_config, None)
        assert loader.doc_url() != loader._doc_base_url, f"{Loader.folder_name} is missing doc_url deep link"
        assert self.check_url(loader.doc_url()), f"{Loader.folder_name} doc_url is not accessible"


def test_resource_types_is_up_to_date() -> None:
    expected = set(LOADER_BY_FOLDER_NAME.keys())
    actual = set(ResourceTypes.__args__)

    missing = expected - actual
    extra = actual - expected
    assert not missing, f"Missing {missing=}"
    assert not extra, f"Extra {extra=}"


def cognite_module_files_with_loader() -> Iterable[ParameterSet]:
    source_path = REPO_ROOT / "cognite_toolkit"
    env = "dev"
    with tmp_build_directory() as build_dir:
        system_config = SystemYAML.load_from_directory(source_path, env)
        config_init = InitConfigYAML(
            Environment(
                name="not used",
                project=os.environ.get("CDF_PROJECT", "<not set>"),
                build_type="dev",
                selected=[],
            )
        ).load_defaults(source_path)
        config = config_init.as_build_config()
        config.set_environment_variables()
        config.environment.selected = config.available_modules

        source_by_build_path = BuildCommand().build_config(
            build_dir=build_dir,
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=True,
            verbose=False,
        )
        for filepath in build_dir.rglob("*.yaml"):
            try:
                resource_folder = resource_folder_from_path(filepath)
            except ValueError:
                # Not a resource file
                continue
            loaders = LOADER_BY_FOLDER_NAME.get(resource_folder, [])
            if not loaders:
                continue
            loader = next((loader for loader in loaders if loader.is_supported_file(filepath)), None)
            if loader is None:
                raise ValueError(f"Could not find loader for {filepath}")
            if loader is FunctionLoader and filepath.parent.name != loader.folder_name:
                # Functions will only accept YAML in root function folder.
                continue
            if issubclass(loader, ResourceLoader):
                raw = yaml.CSafeLoader(filepath.read_text()).get_data()
                source_path = source_by_build_path[filepath]
                module_name = module_from_path(source_path)
                if isinstance(raw, dict):
                    yield pytest.param(loader, raw, id=f"{module_name} - {filepath.stem} - dict")
                elif isinstance(raw, list):
                    for no, item in enumerate(raw):
                        yield pytest.param(loader, item, id=f"{module_name} - {filepath.stem} - list {no}")


class TestResourceLoaders:
    @pytest.mark.parametrize("loader_cls", RESOURCE_LOADER_LIST)
    def test_get_write_cls_spec(self, loader_cls: type[ResourceLoader]):
        resource = FakeCogniteResourceGenerator(seed=1337, max_list_dict_items=1).create_instance(
            loader_cls.resource_write_cls
        )
        resource_dump = resource.dump(camel_case=True)
        # These two are handled by the toolkit
        resource_dump.pop("dataSetId", None)
        resource_dump.pop("fileId", None)
        dumped = read_parameters_from_dict(resource_dump)
        spec = loader_cls.get_write_cls_parameter_spec()

        extra = dumped - spec

        # The spec is calculated based on the resource class __init__ method.
        # There can be deviations in the output from the dump. If that is the case,
        # the 'get_write_cls_parameter_spec' must be updated in the loader. See, for example, the DataModelLoader.
        assert sorted(extra) == []

    @pytest.mark.parametrize("loader_cls, content", list(cognite_module_files_with_loader()))
    def test_write_cls_spec_against_cognite_modules(self, loader_cls: type[ResourceLoader], content: dict) -> None:
        spec = loader_cls.get_write_cls_parameter_spec()

        warnings = validate_resource_yaml(content, spec, Path("test.yaml"))

        assert sorted(warnings) == []

    @pytest.mark.parametrize("loader_cls", RESOURCE_LOADER_LIST)
    def test_empty_required_capabilities_when_no_items(
        self, loader_cls: type[ResourceLoader], cdf_tool_config: CDFToolConfig
    ):
        actual = loader_cls.get_required_capability(loader_cls.list_write_cls([]))

        assert actual == []

    def test_unique_kind_by_folder(self):
        kind = defaultdict(list)
        for loader_cls in RESOURCE_LOADER_LIST:
            kind[loader_cls.folder_name].append(loader_cls.kind)

        duplicated = {folder: Counter(kinds) for folder, kinds in kind.items() if len(set(kinds)) != len(kinds)}
        # we have two types Group loaders, one for scoped and one for all
        # this is intended and thus not an issue.
        duplicated.pop("auth")

        assert not duplicated, f"Duplicated kind by folder: {duplicated!s}"


class TestLoaders:
    def test_unique_display_names(self, cdf_tool_config: CDFToolConfig):
        name_by_count = Counter(
            [loader_cls.create_loader(cdf_tool_config, None).display_name for loader_cls in LOADER_LIST]
        )

        duplicates = {name: count for name, count in name_by_count.items() if count > 1}

        assert not duplicates, f"Duplicate display names: {duplicates}"
