from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.cruds import DataSetsCRUD, GroupAllScopedCRUD, LabelCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitValidationError


class TestValidateUserInput:
    @pytest.mark.parametrize(
        "create_build_dir, options, expected",
        [
            pytest.param(
                False,
                None,
                ToolkitNotADirectoryError,
                id="build_dir_does_not_exist",
            ),
            pytest.param(
                True,
                DeployOptions(include=["not_a_real_folder", "also_invalid"]),
                ToolkitValidationError,
                id="include_contains_invalid_folders",
            ),
        ],
    )
    def test_validate_user_input_raises(
        self,
        create_build_dir: bool,
        options: DeployOptions | None,
        expected: type[Exception],
        tmp_path: Path,
    ) -> None:
        build_dir = tmp_path / "build"
        if create_build_dir:
            build_dir.mkdir(parents=True, exist_ok=True)

        with pytest.raises(expected):
            DeployV2Command._read_build_directory(build_dir, options)


class TestCreateDeploymentPlan:
    def test_empty_build_dir(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        build_dir.mkdir()

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir)

        assert plan.steps == []

    def test_single_resource_folder(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "my.DataSet.yaml").write_text("externalId: ds1")

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir)

        loader_types = [step.loader_cls for step in plan.steps]
        assert DataSetsCRUD in loader_types
        ds_step = next(s for s in plan.steps if s.loader_cls is DataSetsCRUD)
        assert ds_step.files == [ds_dir / "my.DataSet.yaml"]

    def test_include_filters_folders(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        (build_dir / "data_sets").mkdir(parents=True)
        (build_dir / "data_sets" / "my.DataSet.yaml").write_text("externalId: ds1")
        (build_dir / "classic").mkdir(parents=True)
        (build_dir / "classic" / "my.Label.yaml").write_text("externalId: lbl1")

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir, include=["classic"])

        loader_types = [step.loader_cls for step in plan.steps]
        assert LabelCRUD in loader_types
        assert DataSetsCRUD not in loader_types

    def test_topological_order_dependencies_first(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        auth_dir = build_dir / "auth"
        auth_dir.mkdir(parents=True)
        (auth_dir / "my.Group.yaml").write_text("name: g1")

        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "my.DataSet.yaml").write_text("externalId: ds1")

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir)

        loader_types = [step.loader_cls for step in plan.steps]
        assert GroupAllScopedCRUD in loader_types
        assert DataSetsCRUD in loader_types
        assert loader_types.index(GroupAllScopedCRUD) < loader_types.index(DataSetsCRUD)

    def test_folder_without_matching_files_is_skipped(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "unrelated_file.yaml").write_text("externalId: ds1")

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir)

        assert plan.steps == []

    def test_multiple_files_sorted(self, tmp_path: Path) -> None:
        build_dir = tmp_path / "build"
        ds_dir = build_dir / "data_sets"
        ds_dir.mkdir(parents=True)
        (ds_dir / "2.second.DataSet.yaml").write_text("externalId: ds2")
        (ds_dir / "1.first.DataSet.yaml").write_text("externalId: ds1")

        cmd = DeployV2Command(print_warning=False)
        plan = cmd._create_deployment_plan(build_dir)

        ds_step = next(s for s in plan.steps if s.loader_cls is DataSetsCRUD)
        assert ds_step.files == [
            ds_dir / "1.first.DataSet.yaml",
            ds_dir / "2.second.DataSet.yaml",
        ]
