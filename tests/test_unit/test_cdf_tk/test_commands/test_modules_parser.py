from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2._modules_parser import ModulesParser
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from tests.data import COMPLETE_ORG


class TestModulesParser:
    def test_no_folder_raises_error(self) -> None:
        modules_root = Path("missing_module_root")
        with pytest.raises(ToolkitError, match="Module root directory 'missing_module_root/modules' not found"):
            ModulesParser(organization_dir=modules_root).parse()

    def test_load_modules(self) -> None:
        modules, _ = ModulesParser(organization_dir=COMPLETE_ORG).parse()

        assert len(modules) == 3
        assert {module for module in modules} == {
            COMPLETE_ORG / MODULES / "my_example_module",
            COMPLETE_ORG / MODULES / "my_file_expand_module",
            COMPLETE_ORG / MODULES / "populate_model",
        }

    def test_load_selection(self) -> None:
        modules, issues = ModulesParser(
            organization_dir=COMPLETE_ORG, selected=["my_example_module", Path(MODULES) / "my_file_expand_module"]
        ).parse()
        assert len(modules) == 2
        assert {module for module in modules} == {
            COMPLETE_ORG / MODULES / "my_example_module",
            COMPLETE_ORG / MODULES / "my_file_expand_module",
        }
        # No issues should be raised
        assert len(issues) == 0

    @pytest.fixture
    def selection_test_modules_root(self, tmp_path: Path) -> Path:
        modules_root = tmp_path / MODULES

        module_a = modules_root / "A" / "sub" / "module1"
        module_b = modules_root / "B" / "module2"

        for module_dir in (module_a, module_b):
            (module_dir / "transformations").mkdir(parents=True)
            (module_dir / "transformations" / "resource.yaml").touch()

        return modules_root

    def test_selection_default_includes_all_modules(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = ModulesParser(organization_dir=organization_dir).parse()

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest modules should be loaded
        assert {m.relative_to(modules_root) for m in modules} == {
            Path("A/sub/module1"),
            Path("B/module2"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_modules_path_is_recursive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = ModulesParser(organization_dir=organization_dir, selected=[Path(MODULES) / "A"]).parse()

        # Only the selected module should be loaded
        assert {m.relative_to(modules_root) for m in modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_name_is_case_insensitive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = ModulesParser(organization_dir=organization_dir, selected=["A/SUB"]).parse()

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest module should be loaded
        assert {m.relative_to(modules_root) for m in modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_string_path_is_case_insensitive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = ModulesParser(organization_dir=organization_dir, selected=["MODULES/A"]).parse()

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest module should be loaded
        assert {m.relative_to(modules_root) for m in modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_unrecognized_module_gives_warning(self, tmp_path: Path) -> None:
        module_path = tmp_path / MODULES / "mixed_module"
        (module_path / "transformations").mkdir(parents=True)
        (module_path / "transformations" / "transformation.yaml").touch()
        (module_path / "docs").mkdir(parents=True)
        (module_path / "docs" / "readme.md").touch()
        _modules, issues = ModulesParser(organization_dir=tmp_path).parse()

        assert len(issues) == 1
        assert issues[0].message == "Module 'modules/mixed_module' contains unrecognized resource folder(s): docs"

    def test_module_with_normal_and_disabled_resources(self, tmp_path: Path) -> None:
        """Test that a module with both normal and disabled resource folders shows appropriate warnings."""

        module_path = tmp_path / MODULES / "mixed_module"
        (module_path / "transformations").mkdir(parents=True)
        (module_path / "transformations" / "transformation.yaml").touch()
        (module_path / "streams").mkdir(parents=True)
        (module_path / "streams" / "stream.yaml").touch()

        # Create a copy of CRUDS_BY_FOLDER_NAME without streams
        cruds_without_streams = defaultdict(list, {k: v for k, v in CRUDS_BY_FOLDER_NAME.items() if k != "streams"})
        with patch(
            "cognite_toolkit._cdf_tk.commands.build_v2._modules_parser.CRUDS_BY_FOLDER_NAME",
            cruds_without_streams,
        ):
            modules, issues = ModulesParser(organization_dir=tmp_path, selected=["mixed_module"]).parse()

        # The module should be loaded since it has at least one normal resource (transformations)
        assert len(modules) == 1
        assert issues[0].message == "Module 'modules/mixed_module' contains unrecognized resource folder(s): streams"

    def test_module_with_no_resources(self, tmp_path: Path) -> None:
        """Test that a module with no resource folders raises ModuleLoadingNoResourcesIssue and is not loaded."""
        module_path = tmp_path / MODULES / "empty_module"
        (module_path).mkdir(parents=True)
        (module_path / ".gitkeep").touch()
        modules, _ = ModulesParser(organization_dir=tmp_path, selected=["empty_module"]).parse()
        assert len(modules) == 0

    def test_module_container_with_resources_and_nested_module(self, tmp_path: Path) -> None:
        global_path = tmp_path / MODULES / "global"
        (global_path / "transformations").mkdir(parents=True)
        (global_path / "transformations" / "transformation.yaml").touch()

        # Create a nested module inside "global"
        nested_module_path = global_path / "nested_module"
        (nested_module_path / "transformations").mkdir(parents=True)
        (nested_module_path / "transformations" / "transformation.yaml").touch()

        modules, issues = ModulesParser(organization_dir=tmp_path).parse()

        module_paths = {module for module in modules}

        # Only the deepest module should be loaded
        assert len(modules) == 1
        assert nested_module_path in module_paths
        assert global_path not in module_paths

        # The parent module should be discarded with a ModuleLoadingIssue
        assert len(issues) == 1
        assert issues[0].message == "Module 'modules/global' is skipped because it has submodules"

    def test_functions_resource_folder_with_subfolder(self, tmp_path: Path) -> None:
        """Test that a functions resource folder with a subfolder (for function code) is still detected as a resource folder."""
        module_path = tmp_path / MODULES / "my_module"
        functions_path = module_path / "functions"
        functions_path.mkdir(parents=True)

        # Create a function YAML file in the functions folder
        (functions_path / "my_function.yaml").touch()

        # Create a subfolder in functions (for function code)
        code_path = functions_path / "my_function_code"
        code_path.mkdir()
        (code_path / "handler.py").touch()

        modules, issues = ModulesParser(organization_dir=tmp_path).parse()

        # The module should be loaded (functions with subfolders should still be detected)
        assert len(modules) == 1
        assert modules[0] == module_path

        # No issues should be raised (functions folder should be recognized even with subfolders)
        assert len(issues) == 0, f"Expected no issues, but got: {issues}"


class TestGetResourceFolder:
    @pytest.mark.parametrize(
        "resource_file",
        [
            Path("modules/my_module/transformations/transformation.yaml"),
            Path("modules/my_module/data_modeling/views/my_view.yaml"),
            Path("modules/my_module/data_modeling/my_view.yaml"),
            Path("modules/parent/my_module/functions/my_function.yaml"),
        ],
    )
    def test_get_module_folder(self, resource_file: Path) -> None:
        parser = ModulesParser(organization_dir=resource_file.parent)
        result = parser._get_module_path_from_resource_file_path(resource_file)
        assert result.name == "my_module"

    def test_get_resource_folder_arbitrary_yaml_in_subfolder(self, tmp_path: Path) -> None:
        resource_file = tmp_path / MODULES / "my_module" / "functions" / "my_function" / "arbitrary.yaml"
        parser = ModulesParser(organization_dir=tmp_path)
        result = parser._get_module_path_from_resource_file_path(resource_file)
        assert result is None
