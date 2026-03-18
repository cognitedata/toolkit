from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._space import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock, monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    ReadBuildDirectory,
    ResourceDirectory,
    Skipped,
)
from cognite_toolkit._cdf_tk.cruds import (
    CogniteFileCRUD,
    ContainerCRUD,
    DataSetsCRUD,
    FunctionScheduleCRUD,
    LabelCRUD,
    ResourceCRUD,
    SpaceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning


class TestReadBuildDirectory:
    DATA_SET_PATH = "build/data_sets/my.DataSet.yaml"
    DATA_SET_DIR = ResourceDirectory(
        directory=Path("build/data_sets"), files_by_crud={DataSetsCRUD: [Path(DATA_SET_PATH)]}
    )
    LABEL_PATH = "build/classic/my.Label.yaml"
    LABEL_DIR = ResourceDirectory(directory=Path("build/classic"), files_by_crud={LabelCRUD: [Path(LABEL_PATH)]})

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
                    build_dir=Path("build"),
                    resource_directories=[DATA_SET_DIR],
                    skipped_directories=[LABEL_DIR],
                ),
                id="include_filters_to_skipped",
            ),
            pytest.param(
                [DATA_SET_PATH, LABEL_PATH, "build/not_a_valid_resource_type/"],
                None,
                ReadBuildDirectory(
                    build_dir=Path("build"),
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
                    build_dir=Path("build"),
                    resource_directories=[
                        ResourceDirectory(
                            directory=Path("build/data_sets"),
                            files_by_crud={DataSetsCRUD: [Path(DATA_SET_PATH)]},
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
            actual = DeployV2Command.read_build_directory(Path("build"), "test_project", include)
        except Exception as e:
            actual = type(e)

        assert actual == expected


class TestCreateDeploymentPlan:
    @pytest.mark.parametrize(
        "read_dir, expected_plan",
        [
            pytest.param(
                ReadBuildDirectory(build_dir=Path("build")),
                [],
                id="empty_build_directory_produces_empty_plan",
            ),
            pytest.param(
                ReadBuildDirectory(
                    build_dir=Path("build"),
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
                    build_dir=Path("build"),
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
    crud_cls: type[ResourceCRUD]
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
                                    code="DUPLICATED",
                                    source_file=Path("data_modeling/b.Space.yaml"),
                                    reason="Duplicated resource. Will use definition in data_modeling/a.Space.yaml",
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
                    crud_cls=FunctionScheduleCRUD,
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
        elif issubclass(case.crud_cls, FunctionScheduleCRUD):
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
        else:
            pytest.fail(f"Test case for unsupported CRUD class: {case.crud_cls}")
