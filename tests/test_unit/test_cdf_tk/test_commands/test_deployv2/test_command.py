from collections.abc import Sequence
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    ReadBuildDirectory,
    ResourceDirectory,
)
from cognite_toolkit._cdf_tk.cruds import (
    CogniteFileCRUD,
    ContainerCRUD,
    DataSetsCRUD,
    LabelCRUD,
    SpaceCRUD,
)
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
            actual = DeployV2Command.read_build_directory(Path("build"), include)
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


class TestApplyPlan:
    @pytest.mark.parametrize("plan, options, expected", [])
    def test_create_deployment_plan(
        self, plan: list[DeploymentStep], options: DeployOptions, expected: Sequence[DeploymentResult]
    ) -> None:
        with monkeypatch_toolkit_client() as client:
            actual = DeployV2Command.apply_plan(client, plan, options)

        assert actual == expected
