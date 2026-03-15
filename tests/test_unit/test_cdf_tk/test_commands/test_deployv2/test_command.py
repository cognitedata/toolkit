from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    ReadBuildDirectory,
    ResourceDirectory,
    ResourceToDeploy,
)
from cognite_toolkit._cdf_tk.cruds import DataSetsCRUD, GroupAllScopedCRUD, LabelCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitValidationError, ToolkitValueError


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
            actual = DeployV2Command._read_build_directory(Path("build"), include)
        except Exception as e:
            actual = type(e)

        assert actual == expected


class TestCreateDeploymentPlan:
    def test_single_resource(self, tmp_path: Path) -> None:
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[
                ResourceDirectory(
                    directory=tmp_path / "data_sets",
                    files_by_crud={DataSetsCRUD: [tmp_path / "my.DataSet.yaml"]},
                )
            ],
            skipped_directories=[],
            invalid_directories=[],
        )

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(read_dir)

        assert len(plan) == 1
        assert plan[0].crud_cls is DataSetsCRUD
        assert plan[0].files == [tmp_path / "my.DataSet.yaml"]

    def test_topological_order_dependencies_first(self, tmp_path: Path) -> None:
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[
                ResourceDirectory(
                    directory=tmp_path / "data_sets",
                    files_by_crud={DataSetsCRUD: [tmp_path / "my.DataSet.yaml"]},
                ),
                ResourceDirectory(
                    directory=tmp_path / "auth",
                    files_by_crud={GroupAllScopedCRUD: [tmp_path / "my.Group.yaml"]},
                ),
            ],
            skipped_directories=[],
            invalid_directories=[],
        )

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(read_dir)

        crud_types = [step.crud_cls for step in plan]
        assert GroupAllScopedCRUD in crud_types
        assert DataSetsCRUD in crud_types
        assert crud_types.index(GroupAllScopedCRUD) < crud_types.index(DataSetsCRUD)

    def test_warns_on_skipped_dependency(self, tmp_path: Path) -> None:
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[
                ResourceDirectory(
                    directory=tmp_path / "data_sets",
                    files_by_crud={DataSetsCRUD: [tmp_path / "my.DataSet.yaml"]},
                ),
            ],
            skipped_directories=[
                ResourceDirectory(
                    directory=tmp_path / "auth",
                    files_by_crud={GroupAllScopedCRUD: [tmp_path / "my.Group.yaml"]},
                ),
            ],
            invalid_directories=[],
        )

        cmd = DeployV2Command(print_warning=False)
        cmd._create_deployment_plan(read_dir)

        assert len(cmd.warning_list) == 1

    def test_empty_files_by_crud_produces_empty_plan(self, tmp_path: Path) -> None:
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[
                ResourceDirectory(directory=tmp_path / "data_sets"),
            ],
            skipped_directories=[],
            invalid_directories=[],
        )

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(read_dir)

        assert plan == []


class TestDeployDryRun:
    def test_counts_resources(self) -> None:
        resources: ResourceToDeploy[str, str] = ResourceToDeploy(
            to_create=["a", "b"],
            to_delete=["c"],
            to_update=["d", "e", "f"],
            unchanged=["g"],
        )

        result = DeployV2Command.deploy_dry_run(resources)

        assert result == DeploymentResult(
            is_dry_run=True,
            created=2,
            deleted=1,
            updated=3,
            unchanged=1,
        )

    def test_empty_resources(self) -> None:
        resources: ResourceToDeploy[str, str] = ResourceToDeploy()

        result = DeployV2Command.deploy_dry_run(resources)

        assert result == DeploymentResult(
            is_dry_run=True,
            created=0,
            deleted=0,
            updated=0,
            unchanged=0,
        )
