import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import RawDatabaseId, RawTableId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._space import SpaceRequest, SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleData,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabaseResponse, RAWTableResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock, monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    ReadBuildDirectory,
    ResourceDirectory,
    ResourceToDeploy,
    Skipped,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ResourceCreationError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.resource_ios import (
    CogniteFileCRUD,
    ContainerCRUD,
    DataSetsIO,
    FunctionScheduleIO,
    LabelIO,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceIO,
    SpaceCRUD,
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
                                ContainerCRUD: [Path("build/data_modeling/my.Container.yaml")],
                                SpaceCRUD: [Path("build/data_modeling/my.Space.yaml")],
                            },
                        )
                    ],
                ),
                [
                    DeploymentStep(SpaceCRUD, [Path("build/data_modeling/my.Space.yaml")]),
                    DeploymentStep(ContainerCRUD, [Path("build/data_modeling/my.Container.yaml")]),
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
                                CogniteFileCRUD: [Path("build/files/my.CogniteFile.yaml")],
                            },
                        )
                    ],
                    skipped_directories=[
                        ResourceDirectory(
                            directory=Path("build/data_modeling"),
                            files_by_crud={
                                SpaceCRUD: [Path("build/data_modeling/my.Space.yaml")],
                            },
                        )
                    ],
                ),
                [
                    DeploymentStep(
                        CogniteFileCRUD, [Path("build/files/my.CogniteFile.yaml")], skipped_cruds={SpaceCRUD}
                    ),
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
                    crud_cls=SpaceCRUD,
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
                        )
                    ],
                    expected_warning=EnvironmentVariableMissingWarning,
                ),
                id="missing_env_var",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/env.Space.yaml": "name: hello: world"},
                    crud_cls=SpaceCRUD,
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
                    crud_cls=SpaceCRUD,
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
                    crud_cls=SpaceCRUD,
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
                        )
                    ],
                ),
                id="changed_function_schedule_requires_delete_and_update",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: my_space\nname: Updated Name\n"},
                    crud_cls=SpaceCRUD,
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
                        )
                    ],
                ),
                id="changed_space_requires_update",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: new_space\n"},
                    crud_cls=SpaceCRUD,
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
                        )
                    ],
                ),
                id="create_space",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"data_modeling/my.Space.yaml": "space: my_space\n"},
                    crud_cls=SpaceCRUD,
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
                        )
                    ],
                ),
                id="unchanged_space",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"raw/my.Database.yaml": "dbName: my_db\ntableName: my_table\n"},
                    crud_cls=RawDatabaseCRUD,
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
                                    code="HAS-DATA",
                                    source_file=Path("raw/my.Database.yaml"),
                                    reason="name='my_db' contains data and does not support updates. ",
                                )
                            ],
                        )
                    ],
                    expected_skipped_count=1,
                ),
                id="raw_database_with_extra_fields_is_skipped_not_attempted_deleted",
            ),
            pytest.param(
                ApplyPlanTestCase(
                    yaml_files={"raw/my.Table.yaml": "dbName: my_db\ntableName: my_table\nextraField: extra_value\n"},
                    crud_cls=RawTableCRUD,
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
                                    code="HAS-DATA",
                                    source_file=Path("raw/my.Table.yaml"),
                                    reason="my_db.my_table contains data and does not support updates. ",
                                )
                            ],
                        )
                    ],
                    expected_skipped_count=1,
                ),
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

        if issubclass(case.crud_cls, SpaceCRUD):
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
        elif issubclass(case.crud_cls, RawDatabaseCRUD):
            client.tool.raw.databases.list.return_value = case.cdf_resources
        elif issubclass(case.crud_cls, RawTableCRUD):
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
        crud = SpaceCRUD.create_loader(client)

        resources: ResourceToDeploy[SpaceId, SpaceRequest] = ResourceToDeploy()
        resources.to_create = [SpaceRequest(space="my_space")]

        # Mock the spaces API to return an unexpected response structure
        # This will cause a pydantic ValidationError when parsing the response
        spaces_url = toolkit_config.create_api_url("/models/spaces")

        with respx.mock() as mock_router:
            mock_router.post(spaces_url).mock(
                return_value=httpx.Response(
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
