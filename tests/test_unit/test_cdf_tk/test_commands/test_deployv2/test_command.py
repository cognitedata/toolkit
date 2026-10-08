import io
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, patch

import httpx2
import pytest
import respx
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import RawDatabaseId, RawTableId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._space import SpaceRequest, SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleData,
    FunctionScheduleId,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    DataModelsAcl,
    DataSetsAcl,
    DataSetScope,
    IDScope,
    TimeSeriesAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabaseResponse, RAWTableResponse
from cognite_toolkit._cdf_tk.client.resource_classes.token import AllProjects, InspectCapability
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock, monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildLineage
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, InsightList
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._lineage import ModuleLineageItem, ResourceLineageItem
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    ReadBuildDirectory,
    ReadResource,
    ResourceDirectory,
    ResourceToDeploy,
    Skipped,
)
from cognite_toolkit._cdf_tk.constants import DRY_RUN_ID, URL
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ResourceCreationError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    AssetIO,
    CogniteFileIO,
    ContainerIO,
    DataSetsIO,
    FunctionScheduleIO,
    LabelIO,
    RawDatabaseIO,
    RawTableIO,
    ResourceIO,
    SpaceIO,
    TimeSeriesIO,
)
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning


class TestReadBuildDirectory:
    DATA_SET_PATH = "build/data_sets/my.DataSet.yaml"
    DATA_SET_DIR = ResourceDirectory(
        directory=Path("build/data_sets"), files_by_crud={DataSetsIO: [Path(DATA_SET_PATH)]}
    )
    LABEL_PATH = "build/classic/my.Label.yaml"
    LABEL_DIR = ResourceDirectory(directory=Path("build/classic"), files_by_crud={LabelIO: [Path(LABEL_PATH)]})

    @pytest.mark.parametrize(
        "build_files_and_dir, include, expected",
        [
            pytest.param(
                [],
                None,
                ToolkitNotADirectoryError,
                id="build_dir_does_not_exist",
            ),
            pytest.param(
                ["build/auth/my.Group.yaml"],
                ["not_a_real_folder", "also_invalid"],
                ToolkitValidationError,
                id="include_contains_invalid_folders",
            ),
            pytest.param(
                ["build/"],
                None,
                ToolkitValueError,
                id="raises_if_no_resources_found",
            ),
            pytest.param(
                [DATA_SET_PATH, LABEL_PATH],
                ["data_sets"],
                ReadBuildDirectory(
                    path=Path("build"),
                    resource_directories=[DATA_SET_DIR],
                    skipped_directories=[LABEL_DIR],
                ),
                id="include_filters_to_skipped",
            ),
            pytest.param(
                [DATA_SET_PATH, LABEL_PATH, "build/not_a_valid_resource_type/"],
                None,
                ReadBuildDirectory(
                    path=Path("build"),
                    resource_directories=[LABEL_DIR, DATA_SET_DIR],
                    invalid_directories=[Path("build/not_a_valid_resource_type/")],
                ),
                id="invalid_directories_tracked",
            ),
            pytest.param(
                [
                    DATA_SET_PATH,
                    "build/data_sets/unrelated.yaml",
                    "build/data_sets/ignored_markdown.md",
                    "build/another_ignored_file.txt",
                ],
                None,
                ReadBuildDirectory(
                    path=Path("build"),
                    resource_directories=[
                        ResourceDirectory(
                            directory=Path("build/data_sets"),
                            files_by_crud={DataSetsIO: [Path(DATA_SET_PATH)]},
                            invalid_files=[Path("build/data_sets/unrelated.yaml")],
                        )
                    ],
                ),
                id="unmatched_yaml_files_are_invalid",
            ),
        ],
    )
    def test_read_build_directory(
        self,
        build_files_and_dir: list[str],
        include: DeployOptions | list[str] | None,
        expected: type[Exception] | ReadBuildDirectory,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cwd = tmp_path
        for relative_path in build_files_and_dir:
            path = cwd / relative_path
            if relative_path.endswith("/"):
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()

        # Patch the current working directory to tmp_path
        monkeypatch.chdir(tmp_path)

        actual: type[Exception] | ReadBuildDirectory
        try:
            actual = DeployV2Command.read_build_directory(Path("build"), include)
            self._standardize(actual)
        except Exception as e:
            actual = type(e)

        if isinstance(expected, ReadBuildDirectory):
            self._standardize(expected)

        assert actual == expected

    def _standardize(self, read_dir: ReadBuildDirectory) -> None:
        """The read_build_directory function depends on .glob() that is not deterministic in the order
        it returns files and folders."""
        read_dir.invalid_directories.sort()
        self._standardize_resource_directories(read_dir.resource_directories)
        self._standardize_resource_directories(read_dir.skipped_directories)

    def _standardize_resource_directories(self, resource_directories: list[ResourceDirectory]) -> None:
        resource_directories.sort(key=lambda r: r.directory)
        for dir_ in resource_directories:
            dir_.invalid_files.sort()
            dir_.files_by_crud = {
                key: sorted(value)
                for key, value in sorted(dir_.files_by_crud.items(), key=lambda item: item[0].__name__)
            }


class TestCreateDeploymentPlan:
    @pytest.mark.parametrize(
        "read_dir, expected_plan",
        [
            pytest.param(
                ReadBuildDirectory(path=Path("build")),
                [],
                id="empty_build_directory_produces_empty_plan",
            ),
            pytest.param(
                ReadBuildDirectory(
                    path=Path("build"),
                    resource_directories=[
                        ResourceDirectory(
                            directory=Path("build/data_modeling"),
                            files_by_crud={
                                ContainerIO: [Path("build/data_modeling/my.Container.yaml")],
                                SpaceIO: [Path("build/data_modeling/my.Space.yaml")],
                            },
                        )
                    ],
                ),
                [
                    DeploymentStep(SpaceIO, [Path("build/data_modeling/my.Space.yaml")]),
                    DeploymentStep(ContainerIO, [Path("build/data_modeling/my.Container.yaml")]),
                ],
                id="Topological sorting of dependencies",
            ),
            pytest.param(
                ReadBuildDirectory(
                    path=Path("build"),
                    resource_directories=[
                        ResourceDirectory(
                            directory=Path("build/files"),
                            files_by_crud={
                                CogniteFileIO: [Path("build/files/my.CogniteFile.yaml")],
                            },
                        )
                    ],
                    skipped_directories=[
                        ResourceDirectory(
                            directory=Path("build/data_modeling"),
                            files_by_crud={
                                SpaceIO: [Path("build/data_modeling/my.Space.yaml")],
                            },
                        )
                    ],
                ),
                [
                    DeploymentStep(CogniteFileIO, [Path("build/files/my.CogniteFile.yaml")], skipped_cruds={SpaceIO}),
                ],
                id="Skipped potential dependency",
            ),
        ],
    )
    def test_create_deployment_plan(self, read_dir: ReadBuildDirectory, expected_plan: list[DeploymentStep]) -> None:
        actual_plan = DeployV2Command.create_deployment_plan(read_dir)

        assert actual_plan == expected_plan


@dataclass
class ApplyPlanTestCase:
    yaml_files: dict[str, str]
    crud_cls: type[ResourceIO]
    cdf_resources: list
    acls_missing: bool
    options: DeployOptions
    expected: Sequence[DeploymentResult] | type[Exception]
    expected_warning: type[Warning] | None = None
    expected_skipped_count: int = 0


class TestApplyPlan:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/env.Space.yaml": "space: ${MY_VAR}\n"},
                    crud_cls=SpaceIO,
                    cdf_resources=[],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="spaces",
                            is_dry_run=True,
                            created_count=1,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            created_ids=[SpaceId(space="${MY_VAR}")],
                        )
                    ],
                    expected_warning=EnvironmentVariableMissingWarning,
                ),
                id="missing_env_var",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/env.Space.yaml": "name: hello: world"},
                    crud_cls=SpaceIO,
                    cdf_resources=[],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=ToolkitYAMLFormatError,
                    expected_warning=None,
                ),
                id="invalid_yaml",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: my_space\n"},
                    crud_cls=SpaceIO,
                    cdf_resources=[],
                    acls_missing=True,
                    options=DeployOptions(dry_run=False),
                    expected=AuthorizationError,
                ),
                id="missing_acl_raises",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={
                        "data_modeling/a.Space.yaml": "space: my_space\n",
                        "data_modeling/b.Space.yaml": "space: my_space\n",
                    },
                    crud_cls=SpaceIO,
                    cdf_resources=[],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="spaces",
                            is_dry_run=True,
                            created_count=1,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            created_ids=[SpaceId(space="my_space")],
                            skipped=[
                                Skipped(
                                    id=SpaceId(space="my_space"),
                                    code="AMBIGUOUS",
                                    source_file=Path("data_modeling/b.Space.yaml"),
                                    reason="Identifier is not unique. Will use definition in data_modeling/a.Space.yaml",
                                )
                            ],
                        )
                    ],
                    expected_skipped_count=1,
                ),
                id="duplicated_resource_in_two_files",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={
                        "functions/my.Schedule.yaml": "cronExpression: '* * * * *'\nname: my schedule\nfunctionExternalId: 'my_function'\nauthentication:\n  clientId: test_id\n  clientSecret: test_secret\n"
                    },
                    crud_cls=FunctionScheduleIO,
                    cdf_resources=[
                        FunctionScheduleResponse(
                            id=1,
                            cron_expression="* 2 * * *",
                            when="tomorrow",
                            name="my schedule",
                            function_id=37,
                            function_external_id="my_function",
                            created_time=1,
                        )
                    ],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="function schedules",
                            is_dry_run=True,
                            created_count=1,
                            deleted_count=1,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            created_ids=[FunctionScheduleId(function_external_id="my_function", name="my schedule")],
                            deleted_ids=[FunctionScheduleId(function_external_id="my_function", name="my schedule")],
                        )
                    ],
                ),
                id="changed_function_schedule_requires_delete_and_update",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: my_space\nname: Updated Name\n"},
                    crud_cls=SpaceIO,
                    cdf_resources=[
                        SpaceResponse(
                            space="my_space", name="Original Name", created_time=0, last_updated_time=0, is_global=False
                        )
                    ],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="spaces",
                            is_dry_run=True,
                            created_count=0,
                            deleted_count=0,
                            updated_count=1,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            updated_ids=[SpaceId(space="my_space")],
                        )
                    ],
                ),
                id="changed_space_requires_update",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: new_space\n"},
                    crud_cls=SpaceIO,
                    cdf_resources=[],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="spaces",
                            is_dry_run=True,
                            created_count=1,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            created_ids=[SpaceId(space="new_space")],
                        )
                    ],
                ),
                id="create_space",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: my_space\n"},
                    crud_cls=SpaceIO,
                    cdf_resources=[
                        SpaceResponse(space="my_space", created_time=0, last_updated_time=0, is_global=False)
                    ],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="spaces",
                            is_dry_run=True,
                            created_count=0,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=1,
                            is_missing_write_acl=False,
                            unchanged_ids=[SpaceId(space="my_space")],
                        )
                    ],
                ),
                id="unchanged_space",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"raw/my.Database.yaml": "dbName: my_db\ntableName: my_table\n"},
                    crud_cls=RawDatabaseIO,
                    cdf_resources=[RAWDatabaseResponse(name="my_db", created_time=0)],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="raw databases",
                            is_dry_run=True,
                            created_count=0,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            skipped=[
                                Skipped(
                                    id=RawDatabaseId(name="my_db"),
                                    code="NONEMPTY-RESOURCE",
                                    source_file=Path("raw/my.Database.yaml"),
                                    reason="name='my_db' contains data and does not support updates.",
                                )
                            ],
                        )
                    ],
                    expected_skipped_count=1,
                ),
                marks=pytest.mark.skipif(not Flags.V09.is_enabled(), reason="V09 feature flag is not enabled"),
                id="raw_database_with_extra_fields_is_skipped_not_attempted_deleted",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"raw/my.Table.yaml": "dbName: my_db\ntableName: my_table\nextraField: extra_value\n"},
                    crud_cls=RawTableIO,
                    cdf_resources=[RAWTableResponse(db_name="my_db", name="my_table", created_time=0)],
                    acls_missing=False,
                    options=DeployOptions(dry_run=True),
                    expected=[
                        DeploymentResult(
                            resource_name="raw tables",
                            is_dry_run=True,
                            created_count=0,
                            deleted_count=0,
                            updated_count=0,
                            unchanged_count=0,
                            is_missing_write_acl=False,
                            skipped=[
                                Skipped(
                                    id=RawTableId(db_name="my_db", name="my_table"),
                                    code="NONEMPTY-RESOURCE",
                                    source_file=Path("raw/my.Table.yaml"),
                                    reason="my_db.my_table contains data and does not support updates.",
                                )
                            ],
                        )
                    ],
                    expected_skipped_count=1,
                ),
                marks=pytest.mark.skipif(not Flags.V09.is_enabled(), reason="V09 feature flag is not enabled"),
                id="raw_table_with_extra_fields_is_skipped_not_attempted_deleted",
            ),
        ],
    )
    def test_apply_plan(self, case: ApplyPlanTestCase, tmp_path: Path) -> None:
        to_replace: dict[str, str] = {}
        for rel_path, content in case.yaml_files.items():
            path = tmp_path / rel_path
            to_replace[path.as_posix()] = rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

        plan = [DeploymentStep(case.crud_cls, [tmp_path / p for p in case.yaml_files])]

        with monkeypatch_toolkit_client() as client:
            self._set_up_mock_client(case, client)

            actual: Sequence[DeploymentResult] | type[Exception]
            error_message = ""
            try:
                actual = DeployV2Command.apply_plan(client, plan, case.options)
            except Exception as e:
                actual = type(e)
                error_message = str(e)

        if isinstance(actual, list):
            self._replace_absolute_paths(actual, to_replace, tmp_path)

        assert actual == case.expected, error_message

    def _replace_absolute_paths(self, actual: list[DeploymentResult], to_replace: dict[str, str], tmp_path: Path):
        """Cleanup to ensure that the test assertions can use relative paths instead of absolute paths that
        are generated during the test setup."""
        for item in actual:
            for skipped in item.skipped:
                skipped.source_file = skipped.source_file.relative_to(tmp_path)
                for full_path, rel_path in to_replace.items():
                    skipped.reason = skipped.reason.replace(full_path, rel_path)

    def _set_up_mock_client(self, case: ApplyPlanTestCase, client: ToolkitClientMock):
        if case.acls_missing:
            client.tool.token.verify_acls.return_value = [MagicMock()]
            client.tool.token.create_error.return_value = AuthorizationError("Missing capabilities")
        else:
            client.tool.token.verify_acls.return_value = []

        if issubclass(case.crud_cls, SpaceIO):
            client.tool.spaces.retrieve.return_value = case.cdf_resources
        elif issubclass(case.crud_cls, FunctionScheduleIO):
            client.functions.status.return_value.status = "activated"
            function_responses = []
            for resource in case.cdf_resources:
                if hasattr(resource, "function_id") and resource.function_id is not None:
                    function_responses.append(
                        FunctionResponse(
                            id=resource.function_id,
                            external_id=resource.function_external_id,
                            name="myfunction",
                            created_time=1,
                            file_id=37,
                        )
                    )
            client.tool.functions.retrieve.return_value = function_responses
            client.tool.functions.schedules.list.return_value = case.cdf_resources
            client.tool.functions.schedules.input_data.return_value = FunctionScheduleData(id=37)
        elif issubclass(case.crud_cls, RawDatabaseIO):
            client.tool.raw.databases.list.return_value = case.cdf_resources
        elif issubclass(case.crud_cls, RawTableIO):
            client.tool.raw.tables.list.return_value = case.cdf_resources
        else:
            pytest.fail(f"Test case for unsupported CRUD class: {case.crud_cls}")


class TestDeployResourcesValidationError:
    """Tests for handling pydantic ValidationError when the API returns an unexpected response."""

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_deploy_resources_raises_resource_creation_error_on_unexpected_api_response(
        self, toolkit_config: ToolkitClientConfig, tmp_path: Path
    ) -> None:
        """Test that deploy_resources raises ResourceCreationError when the API returns an unexpected response.

        This tests the _handle_validation_error method by mocking the spaces API to return
        an unexpected JSON structure that will cause a pydantic ValidationError.
        """
        client = ToolkitClient(config=toolkit_config)
        crud = SpaceIO.create_io(client)

        resources: ResourceToDeploy[SpaceId, SpaceRequest] = ResourceToDeploy()
        resources.to_create = [SpaceRequest(space="my_space")]

        # Mock the spaces API to return an unexpected response structure
        # This will cause a pydantic ValidationError when parsing the response
        spaces_url = toolkit_config.create_api_url("/models/spaces")

        with respx.mock() as mock_router:
            mock_router.post(spaces_url).mock(
                return_value=httpx2.Response(
                    status_code=200,
                    # Missing space
                    json={"items": [{"createdTime": 0, "lastUpdatedTime": 1, "isGlobal": False}]},
                )
            )

            with pytest.raises(ResourceCreationError) as exc_info:
                DeployV2Command.deploy_resources(crud, resources, skipped_cruds=set(), deploy_dir=tmp_path)

        assert "unexpected CDF API response" in str(exc_info.value)
        assert "spaces" in str(exc_info.value)
        debug_file = list(tmp_path.glob("*.json"))
        assert len(debug_file) == 1
        dumped = json.loads(debug_file[0].read_text())
        assert len(dumped) == 1
        # Remove URL as it depends on version of pydantic
        dumped[0].pop("url")
        assert dumped == [
            {
                "input": {"createdTime": 0, "isGlobal": False, "lastUpdatedTime": 1},
                "loc": ["items", 0, "space"],
                "msg": "Field required",
                "type": "missing",
            }
        ]


class TestDeployResourcesRetryDump:
    """Tests that a retried create failure dumps the request, including which status codes were retried."""

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_create_dumps_request_after_502_retry_then_409(
        self, toolkit_config: ToolkitClientConfig, tmp_path: Path
    ) -> None:
        """A 502 is retried once, then a 409 fails and the request is written with retriedStatusCodes [502]."""
        client = ToolkitClient(config=toolkit_config)
        crud = SpaceIO.create_io(client)

        resources: ResourceToDeploy[SpaceId, SpaceRequest] = ResourceToDeploy()
        resources.to_create = [SpaceRequest(space="my_space")]

        spaces_url = toolkit_config.create_api_url("/models/spaces")

        with respx.mock() as mock_router:
            mock_router.post(spaces_url).mock(
                side_effect=[
                    httpx2.Response(status_code=502, json={"error": {"code": 502, "message": "Bad Gateway"}}),
                    httpx2.Response(status_code=409, json={"error": {"code": 409, "message": "Conflict"}}),
                ]
            )

            with patch("time.sleep"), pytest.raises(ResourceCreationError):
                DeployV2Command.deploy_resources(crud, resources, skipped_cruds=set(), deploy_dir=tmp_path)

            assert len(mock_router.calls) == 2

        debug_files = list(tmp_path.glob("*.json"))
        assert len(debug_files) == 1
        dumped = json.loads(debug_files[0].read_text(encoding="utf-8"))
        assert dumped["retriedStatusCodes"] == [502]
        assert dumped["requestBody"] == {"items": [{"space": "my_space"}]}
        assert dumped["method"] == "POST"
        assert dumped["url"] == spaces_url
        assert dumped["statusCode"] == 409


class TestDetectKeyColumn:
    """Unit tests for DeployV2Command._detect_key_column."""

    def test_csv_first_column_named_key_returns_key(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "my_table.csv"
        csv_file.write_text("key,name,value\n1,foo,bar\n2,baz,qux\n", encoding="utf-8")
        assert DeployV2Command._detect_key_column(csv_file) == "key"

    def test_csv_first_column_not_named_key_returns_none(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "my_table.csv"
        csv_file.write_text("id,name,value\n1,foo,bar\n", encoding="utf-8")
        assert DeployV2Command._detect_key_column(csv_file) is None

    def test_csv_empty_file_returns_none(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("", encoding="utf-8")
        assert DeployV2Command._detect_key_column(csv_file) is None

    def test_unknown_suffix_returns_none(self, tmp_path: Path) -> None:
        txt_file = tmp_path / "my_table.txt"
        txt_file.write_text("key,name\n1,foo\n", encoding="utf-8")
        assert DeployV2Command._detect_key_column(txt_file) is None


class TestCategorizeResources:
    def test_dropping_resource_not_supporting_delete(self, toolkit_client_cheap: ToolkitClient) -> None:
        dataset_raw = {"externalId": "my_dataset", "name": "My DataSet"}
        request = DataSetRequest.model_validate(dataset_raw)
        result = DeployV2Command.categorize_resources(
            DataSetsIO.create_io(toolkit_client_cheap),
            resource_by_id={
                request.as_id(): ReadResource(request, dataset_raw, [MagicMock()]),
            },
            cdf_by_id={
                request.as_id(): DataSetResponse.model_validate(
                    {"id": 1, "lastUpdatedTime": 1, "createdTime": 1, **dataset_raw}
                )
            },
            is_delete=True,
        )
        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
            "skipped": len(result.skipped),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 0, "skipped": 1}


def _space_lineage_with_insights(
    tmp_path: Path, fmt: Literal["json", "csv"]
) -> tuple[Path, BuildLineage, InsightList, SpaceId]:
    organization_dir = tmp_path / "org"
    build_dir = tmp_path / "build"
    source_file = organization_dir / "modules" / "my_module" / "data_modeling" / "my.Space.yaml"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("space: my_space\n", encoding="utf-8")
    build_dir.mkdir()
    space_id = SpaceId(space="my_space")
    lineage = BuildLineage(
        organization_dir=organization_dir,
        build_dir=build_dir,
        modules_summary={"processed": 1, "succeeded": 1, "failed": 0},
        insights_summary={},
        module_lineage=[
            ModuleLineageItem(
                module_id="modules/my_module",
                module_path=Path("modules/my_module"),
                insights_summary={},
                resource_lineage=[
                    ResourceLineageItem(
                        source_file=source_file.relative_to(organization_dir),
                        source_hash="abc",
                        type={"resource_folder": "data_modeling", "kind": "Space"},
                        built_file=Path("data_modeling/my.Space.yaml"),
                        identifier={"space": "my_space"},
                    )
                ],
            )
        ],
    )
    insights = InsightList(
        [
            ConsistencyError(
                message="Space is fine this is a test",
                code="NOT-REAL",
                source_file=source_file,
                fix="Cannot be fixed as it is not an issue",
            )
        ]
    )
    insight_content = insights.to_csv() if fmt == "csv" else insights.to_json()
    (build_dir / f"insights.{fmt}").write_text(insight_content, encoding="utf-8")
    return build_dir, lineage, insights, space_id


class TestReadInsightsByResource:
    @pytest.mark.parametrize("fmt", ["csv", "json"])
    def test_maps_insights_to_resources_from_file(self, tmp_path: Path, fmt: Literal["csv", "json"]) -> None:
        build_dir, lineage, insights, space_id = _space_lineage_with_insights(tmp_path, fmt)

        actual = DeployV2Command.read_insights_by_resource(build_dir, lineage)

        key = (SpaceIO.as_resource_type(), space_id)
        assert actual is not None
        assert key in actual
        actual_insights = actual[key]
        assert [insight.model_dump() for insight in actual_insights] == insights.dump()

    def test_returns_none_when_insights_file_missing(self, tmp_path: Path) -> None:
        build_dir, lineage, *_ = _space_lineage_with_insights(tmp_path, "csv")
        (build_dir / "insights.csv").unlink()

        assert DeployV2Command.read_insights_by_resource(build_dir, lineage) is None

    def test_returns_none_when_lineage_missing(self, tmp_path: Path) -> None:
        build_dir, *_ = _space_lineage_with_insights(tmp_path, "csv")

        assert DeployV2Command.read_insights_by_resource(build_dir, None) is None


class TestDeployResourcesRelatedInsights:
    def test_api_error_includes_related_insights(self, tmp_path: Path) -> None:
        build_dir, lineage, _, space_id = _space_lineage_with_insights(tmp_path, "json")

        insights_by_resource = DeployV2Command.read_insights_by_resource(build_dir, lineage)

        resources = ResourceToDeploy[SpaceId, SpaceRequest]()
        resources.to_create = [SpaceRequest(space=space_id.space)]

        console_output = io.StringIO()
        client = MagicMock()
        client.console = Console(file=console_output, width=200)
        client.tool.spaces.create.side_effect = ToolkitAPIError("API failed")
        crud = SpaceIO.create_io(client)
        with pytest.raises(ResourceCreationError, match="Likely causes detected during"):
            DeployV2Command.deploy_resources(
                crud, resources, skipped_cruds=set(), insights_by_resource=insights_by_resource
            )

        # The insight details are rendered in a prominent panel rather than the exception message.
        output = console_output.getvalue()
        assert "NOT-REAL" in output
        assert "Space is fine this is a test" in output
        assert "Suggested fix:" in output
        assert "Cannot be fixed as it is not an issue" in output


def _inspect_capability(acl: TimeSeriesAcl) -> dict[str, object]:
    return InspectCapability(acl=acl, project_scope=AllProjects(all_projects={})).dump()


class TestDryRunDataSetPlaceholders:
    def test_write_protected_datasets_skips_dry_run_id(self) -> None:
        client = MagicMock()
        assert DeployV2Command._write_protected_datasets(client, [DRY_RUN_ID]) == set()
        client.tool.datasets.retrieve.assert_not_called()

    def test_validate_access_dry_run_never_retrieves_dry_run_dataset(self) -> None:
        client = MagicMock()
        client.tool.token.verify_acls.return_value = []
        loader = AssetIO.create_io(client)
        resource = AssetRequest(name="my_asset", external_id="my_asset", data_set_id=DRY_RUN_ID)
        is_missing_read, is_missing_write, is_write_acl_unknown = DeployV2Command._validate_access(
            loader, [resource], client, is_dry_run=True
        )
        client.tool.datasets.retrieve.assert_not_called()
        assert is_missing_read is False
        assert is_missing_write is False
        assert is_write_acl_unknown is True

    def test_format_able_to_deploy_unknown(self) -> None:
        result = DeploymentResult(
            resource_name="assets",
            is_dry_run=True,
            created_count=0,
            deleted_count=0,
            updated_count=0,
            unchanged_count=0,
            is_missing_write_acl=False,
            is_write_acl_unknown=True,
        )
        assert DeployV2Command._format_able_to_deploy(result) == "[yellow]Unknown[/]"


class TestDeployAccessControlErrors:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_empty_inspect_response_raises_authorization_error(
        self, toolkit_config: ToolkitClientConfig, tmp_path: Path, respx_mock: respx.MockRouter
    ) -> None:
        yaml_file = tmp_path / "my.Space.yaml"
        yaml_file.write_text("space: my_space\n")
        respx_mock.get(f"{toolkit_config.base_url}/api/v1/token/inspect").respond(
            json={"subject": "test", "projects": [], "capabilities": []}
        )
        client = ToolkitClient(config=toolkit_config)

        with pytest.raises(AuthorizationError) as exc_info:
            DeployV2Command.apply_plan(client, [DeploymentStep(SpaceIO, [yaml_file])], DeployOptions(dry_run=False))

        required = DataModelsAcl(actions=["READ", "WRITE"], scope=AllScope())
        assert str(exc_info.value) == (
            f"Failed to validate {required!r}. \n"
            f"Missing project '{toolkit_config.project}' in inspect response. "
            "You are likely not a member of a group with ProjectsAcl and GroupsAcl capabilities with LIST action."
        )

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_write_protected_dataset_raises_authorization_error(
        self, toolkit_config: ToolkitClientConfig, tmp_path: Path, respx_mock: respx.MockRouter
    ) -> None:
        data_set_id = 42
        yaml_file = tmp_path / "my.TimeSeries.yaml"
        yaml_file.write_text(f"externalId: my_ts\nname: My TS\ndataSetId: {data_set_id}\n")
        respx_mock.get(f"{toolkit_config.base_url}/api/v1/token/inspect").respond(
            json={
                "subject": "test",
                "projects": [{"projectUrlName": toolkit_config.project, "groups": [1]}],
                "capabilities": [
                    _inspect_capability(TimeSeriesAcl(actions=["READ", "WRITE"], scope=DataSetScope(ids=[data_set_id])))
                ],
            }
        )
        respx_mock.post(toolkit_config.create_api_url("/datasets/byids")).respond(
            json={
                "items": [
                    DataSetResponse(
                        id=data_set_id,
                        external_id="my_dataset",
                        write_protected=True,
                        created_time=1,
                        last_updated_time=1,
                    ).dump()
                ]
            }
        )
        create_route = respx_mock.post(toolkit_config.create_api_url("/timeseries")).respond(
            status_code=403, json={"error": {"message": "Forbidden"}}
        )
        client = ToolkitClient(config=toolkit_config)

        with pytest.raises(AuthorizationError) as exc_info:
            DeployV2Command.apply_plan(
                client, [DeploymentStep(TimeSeriesIO, [yaml_file])], DeployOptions(dry_run=False)
            )

        missing_owner = DataSetsAcl(actions=["OWNER"], scope=IDScope(ids=[data_set_id]))
        assert {
            "message": str(exc_info.value),
            "create_called": create_route.called,
        } == {
            "message": (
                "Don't have correct access rights to deploy time series. Missing:\n"
                f"  - {missing_owner!r}\n"
                f"Please [blue][link={URL.auth_toolkit}]click here[/link][/blue] to visit the documentation "
                "and ensure that you have setup authentication for the CDF toolkit correctly."
            ),
            "create_called": False,
        }


def _render_results(results: Sequence[DeploymentResult], *, verbose: bool, width: int = 100) -> str:
    buffer = io.StringIO()
    console = Console(
        file=buffer,
        width=width,
        height=80,
        legacy_windows=False,
        force_terminal=True,
        color_system=None,
        highlight=False,
    )
    DeployV2Command._display_results(results, "deploy", console, verbose)
    return buffer.getvalue()


class TestVerboseResourceOutcomes:
    def test_verbose_lists_each_outcome_and_quiet_mode_does_not(self) -> None:
        schedule = FunctionScheduleId(function_external_id="my_function", name="my schedule")
        results = [
            DeploymentResult(
                resource_name="spaces",
                is_dry_run=True,
                created_count=1,
                deleted_count=1,
                updated_count=1,
                unchanged_count=1,
                is_missing_write_acl=False,
                created_ids=[SpaceId(space="new_space")],
                updated_ids=[SpaceId(space="edited_space")],
                deleted_ids=[SpaceId(space="old_space")],
                unchanged_ids=[SpaceId(space="same_space")],
                skipped=[
                    Skipped(
                        id=SpaceId(space="dup_space"),
                        code="AMBIGUOUS",
                        source_file=Path("data_modeling/b.Space.yaml"),
                        reason="Identifier is not unique",
                    )
                ],
            ),
            DeploymentResult(
                resource_name="function schedules",
                is_dry_run=True,
                created_count=1,
                deleted_count=1,
                updated_count=0,
                unchanged_count=0,
                is_missing_write_acl=False,
                created_ids=[schedule],
                deleted_ids=[schedule],
            ),
        ]

        verbose_output = " ".join(_render_results(results, verbose=True).split())
        quiet_output = " ".join(_render_results(results, verbose=False).split())

        assert {
            "panel": "Resources" in verbose_output and "(dry run)" in verbose_output,
            "would_create": "would create" in verbose_output,
            "would_update": "would update" in verbose_output,
            "would_delete": "would delete" in verbose_output,
            "created_id": "new_space" in verbose_output,
            "updated_id": "edited_space" in verbose_output,
            "deleted_id": "old_space" in verbose_output,
            "unchanged_id": "same_space" in verbose_output,
            "skipped_id": "dup_space" in verbose_output,
            "skipped_code": "AMBIGUOUS" in verbose_output,
            "skipped_file": "b.Space.yaml" in verbose_output,
            "schedule_id": "my_function" in verbose_output and "my schedule" in verbose_output,
            "quiet_hides_ids": "new_space" not in quiet_output and "dup_space" not in quiet_output,
            "quiet_points_at_verbose": "list each resource and its outcome" in quiet_output,
        } == {
            "panel": True,
            "would_create": True,
            "would_update": True,
            "would_delete": True,
            "created_id": True,
            "updated_id": True,
            "deleted_id": True,
            "unchanged_id": True,
            "skipped_id": True,
            "skipped_code": True,
            "skipped_file": True,
            "schedule_id": True,
            "quiet_hides_ids": True,
            "quiet_points_at_verbose": True,
        }

    def test_verbose_keeps_every_id_when_there_are_many(self) -> None:
        unchanged = [SpaceId(space=f"space_{index:03d}") for index in range(80)]
        long_id = "asset_" + ("external-id-" * 20)
        results = [
            DeploymentResult(
                resource_name="spaces",
                is_dry_run=False,
                created_count=1,
                deleted_count=0,
                updated_count=0,
                unchanged_count=len(unchanged),
                is_missing_write_acl=False,
                created_ids=[SpaceId(space=long_id)],
                unchanged_ids=unchanged,
            )
        ]

        output = " ".join(_render_results(results, verbose=False, width=72).split())
        verbose_output = " ".join(_render_results(results, verbose=True, width=72).split())

        assert {
            "lists_every_unchanged_id": all(f"space_{index:03d}" in verbose_output for index in range(80)),
            "keeps_long_id": long_id in verbose_output.replace("│", "").replace(" ", ""),
            "sorts_ids": verbose_output.index("space_002") < verbose_output.index("space_010"),
            "quiet_omits_ids": "space_000" not in output,
            "live_label": "created" in verbose_output and "would create" not in verbose_output,
        } == {
            "lists_every_unchanged_id": True,
            "keeps_long_id": True,
            "sorts_ids": True,
            "quiet_omits_ids": True,
            "live_label": True,
        }

    def test_dry_run_drop_reclassifies_unchanged_and_updated_ids(self) -> None:
        crud = MagicMock()
        crud.display_name = "spaces"
        crud.support_drop = True
        crud.get_id.side_effect = lambda resource: resource
        new = SpaceId(space="new")
        edited = SpaceId(space="edited")
        same = SpaceId(space="same")
        old = SpaceId(space="old")
        resources = ResourceToDeploy(
            to_create=[new],
            to_update=[edited],
            to_delete=[old],
            unchanged=[same],
        )

        result = DeployV2Command.deploy_dry_run(
            crud,
            resources,
            is_missing_write_acl=False,
            is_write_acl_unknown=False,
            options=DeployOptions(dry_run=True, drop=True),
        )

        assert {
            "created_ids": result.created_ids,
            "deleted_ids": result.deleted_ids,
            "updated_ids": result.updated_ids,
            "unchanged_ids": result.unchanged_ids,
            "created_count": result.created_count,
            "deleted_count": result.deleted_count,
        } == {
            "created_ids": [new, same, edited],
            "deleted_ids": [old, same, edited],
            "updated_ids": [],
            "unchanged_ids": [],
            "created_count": 3,
            "deleted_count": 3,
        }

    def test_deploy_resources_records_ids(self) -> None:
        crud = MagicMock()
        crud.display_name = "spaces"
        crud.get_id.side_effect = lambda resource: resource
        created = SpaceId(space="new")
        updated = SpaceId(space="edited")
        deleted = SpaceId(space="old")
        unchanged = SpaceId(space="same")
        resources: ResourceToDeploy[SpaceId, SpaceRequest] = ResourceToDeploy(
            to_create=[created],
            to_update=[updated],
            to_delete=[deleted],
            unchanged=[unchanged],
        )
        crud.delete.return_value = 1
        crud.create.return_value = [created]
        crud.update.return_value = [updated]

        result = DeployV2Command.deploy_resources(crud, resources, skipped_cruds=set())

        assert {
            "created_ids": result.created_ids,
            "updated_ids": result.updated_ids,
            "deleted_ids": result.deleted_ids,
            "unchanged_ids": result.unchanged_ids,
            "created_count": result.created_count,
            "updated_count": result.updated_count,
            "deleted_count": result.deleted_count,
        } == {
            "created_ids": [created],
            "updated_ids": [updated],
            "deleted_ids": [deleted],
            "unchanged_ids": [unchanged],
            "created_count": 1,
            "updated_count": 1,
            "deleted_count": 1,
        }
