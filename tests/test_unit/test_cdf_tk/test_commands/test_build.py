from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.build import BuildCommand, _BuildState
from cognite_toolkit._cdf_tk.data_classes import Environment
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from tests import data


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )


class TestBuildCommand:
    def test_get_loader_raises_ambiguous_error(self):
        with pytest.raises(AmbiguousResourceFileError) as e:
            BuildCommand()._get_loader(
                "transformations",
                destination=Path("transformation") / "notification.yaml",
                source_path=Path("my_module") / "transformations" / "notification.yaml",
            )
        assert "Ambiguous resource file" in str(e.value)

    def test_module_not_found_error(self, tmp_path: Path) -> None:
        with pytest.raises(ToolkitMissingModuleError):
            BuildCommand(print_warning=False).execute(
                verbose=False,
                build_dir=tmp_path,
                source_path=data.PROJECT_WITH_BAD_MODULES,
                build_env_name="no_module",
                no_clean=False,
            )

    def test_module_with_non_resource_directories(self, tmp_path: Path) -> None:
        cmd = BuildCommand(print_warning=False)
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            source_path=data.PROJECT_WITH_BAD_MODULES,
            build_env_name="ill_module",
            no_clean=False,
        )

        assert len(cmd.warning_list) >= 1
        assert (
            LowSeverityWarning(
                f"Module 'ill_made_module' has non-resource directories: ['spaces']. {ModuleDefinition.short()}"
            )
            in cmd.warning_list
        )

    def test_custom_project_no_warnings(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        cmd = BuildCommand(print_warning=False)
        monkeypatch.setenv("CDF_PROJECT", "some-project")
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            source_path=data.PROJECT_NO_COGNITE_MODULES,
            build_env_name="dev",
            no_clean=False,
        )

        assert not cmd.warning_list, f"No warnings should be raised. Got warnings: {cmd.warning_list}"
        # There are two transformations in the project, expect two transformation files
        transformation_files = [
            f
            for f in (tmp_path / "transformations").iterdir()
            if f.is_file() and TransformationLoader.is_supported_file(f)
        ]
        assert len(transformation_files) == 2
        sql_files = [f for f in (tmp_path / "transformations").iterdir() if f.is_file() and f.suffix == ".sql"]
        assert len(sql_files) == 2


def valid_yaml_semantics_test_cases() -> Iterable[pytest.ParameterSet]:
    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
- dbName: src:002:weather:rawdb:state
- dbName: uc:001:demand:rawdb:state
- dbName: in:all:rawdb:state
- dbName: src:001:sap:rawdb
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
tableName: myTable
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database with table",
    )

    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
  tableName: myTable
- dbName: src:002:weather:rawdb:state
  tableName: myOtherTable
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases with table",
    )


class TestCheckYamlSemantics:
    @pytest.mark.parametrize("raw_yaml, source_path", list(valid_yaml_semantics_test_cases()))
    def test_valid_yaml(self, raw_yaml: str, source_path: Path, dummy_environment: Environment):
        state = _BuildState()
        cmd = BuildCommand(print_warning=False)
        # Only used in error messages
        destination = Path("build/raw/raw.yaml")
        yaml_warnings = cmd.validate(raw_yaml, source_path, destination, state, {})
        assert not yaml_warnings
