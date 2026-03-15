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
                ["build/auth/my.DataSet.yaml"],
                ["not_a_real_folder", "also_invalid"],
                ToolkitValidationError,
                id="include_contains_invalid_folders",
            ),
        ],
    )
    def test_read_build_directory(
        self,
        build_files_and_dir: list[str],
        include: list[str] | None,
        expected: type[Exception] | ReadBuildDirectory,
        tmp_path: Path,
    ) -> None:
        build_dir = tmp_path / "build"
        for relative_path in build_files_and_dir:
            path = build_dir / relative_path
            path.mkdir(parents=True, exist_ok=True)
            if path.suffix == ".yaml":
                path.touch()

        actual: ReadBuildDirectory | type[Exception]
        try:
            actual = DeployV2Command._read_build_directory(build_dir, include)
        except Exception as e:
            actual = type(e)

        assert actual == expected

    def test_raises_if_no_resources_found(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        build_dir.mkdir()

        with pytest.raises(ToolkitValueError):
            DeployV2Command._read_build_directory(build_dir)

    def test_single_resource_folder(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "my.DataSet.yaml").write_text("externalId: ds1")

        result = DeployV2Command._read_build_directory(build_dir)

        assert len(result.resource_directories) == 1
        assert result.resource_directories[0].directory == ds_dir
        cruds = result.resource_directories[0].files_by_crud
        assert DataSetsCRUD in cruds
        assert cruds[DataSetsCRUD] == [ds_dir / "my.DataSet.yaml"]

    def test_include_filters_to_skipped(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        (build_dir / "data_sets").mkdir(parents=True)
        (build_dir / "data_sets" / "my.DataSet.yaml").write_text("externalId: ds1")
        (build_dir / "classic").mkdir(parents=True)
        (build_dir / "classic" / "my.Label.yaml").write_text("externalId: lbl1")

        result = DeployV2Command._read_build_directory(build_dir, DeployOptions(include=["classic"]))

        included_cruds = result.as_files_by_crud()
        assert LabelCRUD in included_cruds
        assert DataSetsCRUD not in included_cruds
        assert len(result.skipped_directories) == 1

    def test_invalid_directories_tracked(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        (build_dir / "data_sets").mkdir(parents=True)
        (build_dir / "data_sets" / "my.DataSet.yaml").write_text("externalId: ds1")
        (build_dir / "not_a_valid_resource_type").mkdir(parents=True)

        result = DeployV2Command._read_build_directory(build_dir)

        assert len(result.invalid_directories) == 1
        assert result.invalid_directories[0].name == "not_a_valid_resource_type"

    def test_unmatched_yaml_files_are_invalid(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "my.DataSet.yaml").write_text("externalId: ds1")
        (ds_dir / "unrelated.yaml").write_text("externalId: other")

        result = DeployV2Command._read_build_directory(build_dir)

        resource_dir = result.resource_directories[0]
        assert resource_dir.invalid_files == [ds_dir / "unrelated.yaml"]


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


class TestReadBuildDirectoryDataClasses:
    def test_as_files_by_crud_merges_directories(self, tmp_path: Path) -> None:
        file_a = tmp_path / "a.DataSet.yaml"
        file_b = tmp_path / "b.Label.yaml"
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[
                ResourceDirectory(
                    directory=tmp_path / "data_sets",
                    files_by_crud={DataSetsCRUD: [file_a]},
                ),
                ResourceDirectory(
                    directory=tmp_path / "classic",
                    files_by_crud={LabelCRUD: [file_b]},
                ),
            ],
            skipped_directories=[],
            invalid_directories=[],
        )

        merged = read_dir.as_files_by_crud()

        assert merged == {DataSetsCRUD: [file_a], LabelCRUD: [file_b]}

    def test_skipped_cruds(self, tmp_path: Path) -> None:
        read_dir = ReadBuildDirectory(
            build_dir=tmp_path,
            resource_directories=[],
            skipped_directories=[
                ResourceDirectory(
                    directory=tmp_path / "auth",
                    files_by_crud={GroupAllScopedCRUD: [tmp_path / "my.Group.yaml"]},
                ),
            ],
            invalid_directories=[],
        )

        assert read_dir.skipped_cruds() == {GroupAllScopedCRUD}
