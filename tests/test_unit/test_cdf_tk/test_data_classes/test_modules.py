from pathlib import Path
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._modules import Modules
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes._issues import (
    ModuleLoadingIssue,
)
from tests.data import COMPLETE_ORG


class TestModules:
    def test_load_modules(self) -> None:
        modules, _ = Modules.load(COMPLETE_ORG)

        assert len(modules.modules) == 3
        assert {module.path for module in modules.modules} == {
            COMPLETE_ORG / MODULES / "my_example_module",
            COMPLETE_ORG / MODULES / "my_file_expand_module",
            COMPLETE_ORG / MODULES / "populate_model",
        }

    def test_load_selection(self) -> None:
        modules, issues = Modules.load(
            COMPLETE_ORG, selection=["my_example_module", Path(MODULES) / "my_file_expand_module"]
        )

        assert len(modules.modules) == 2
        assert {module.path for module in modules.modules} == {
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

        modules, issues = Modules.load(organization_dir)

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest modules should be loaded
        assert {m.path.relative_to(modules_root) for m in modules.modules} == {
            Path("A/sub/module1"),
            Path("B/module2"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_modules_path_is_recursive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = Modules.load(organization_dir, selection=[Path(MODULES) / "A"])

        # Only the selected module should be loaded
        assert {m.path.relative_to(modules_root) for m in modules.modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_name_is_case_insensitive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = Modules.load(organization_dir, selection=["A/SUB"])

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest module should be loaded
        assert {m.path.relative_to(modules_root) for m in modules.modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_selection_by_string_path_is_case_insensitive(self, selection_test_modules_root: Path) -> None:
        modules_root = selection_test_modules_root
        organization_dir = modules_root.parent

        modules, issues = Modules.load(organization_dir, selection=["MODULES/A"])

        # A is a parent module with nested modules, so it should be discarded
        # Only the deepest module should be loaded
        assert {m.path.relative_to(modules_root) for m in modules.modules} == {
            Path("A/sub/module1"),
        }
        # No issues should be raised
        assert len(issues) == 0

    def test_module_root_directory_missing(self, tmp_path: Path) -> None:
        modules_root = Path("missing_module_root")
        _modules, issues = Modules.load(modules_root)
        assert len(issues) == 1
        assert isinstance(issues[0], ModuleLoadingIssue)

    def test_unrecognized_module_gives_warning(self, tmp_path: Path) -> None:
        module_path = tmp_path / MODULES / "mixed_module"
        (module_path / "transformations").mkdir(parents=True)
        (module_path / "transformations" / "transformation.yaml").touch()
        (module_path / "docs").mkdir(parents=True)
        (module_path / "docs" / "readme.md").touch()
        _modules, issues = Modules.load(tmp_path)

        assert len(issues) == 1
        assert issues[0].path == module_path
        assert issues[0].message == f"Module {module_path.as_posix()!r} contains unrecognized resource folders: docs"

    def test_module_with_normal_and_disabled_resources(self, tmp_path: Path) -> None:
        """Test that a module with both normal and disabled resource folders shows appropriate warnings."""
        from cognite_toolkit._cdf_tk.cruds._resource_cruds.streams import StreamCRUD

        module_path = tmp_path / MODULES / "mixed_module"
        (module_path / "transformations").mkdir(parents=True)
        (module_path / "transformations" / "transformation.yaml").touch()
        (module_path / "streams").mkdir(parents=True)
        (module_path / "streams" / "stream.yaml").touch()

        # Mock EXCLUDED_CRUDS to always include StreamCRUD so streams is always disabled
        with patch("cognite_toolkit._cdf_tk.commands.build_v2.data_classes._modules.EXCLUDED_CRUDS", {StreamCRUD}):
            modules, issues = Modules.load(tmp_path, selection=["mixed_module"])

        # The module should be loaded since it has at least one normal resource (transformations)
        assert len(modules.modules) == 1
        assert issues[0].path == module_path
        assert (
            issues[0].message
            == f"Module {module_path.as_posix()!r} contains unsupported resource folders, check flags in cdf.toml: streams"
        )

    def test_module_with_no_resources(self, tmp_path: Path) -> None:
        """Test that a module with no resource folders raises ModuleLoadingNoResourcesIssue and is not loaded."""
        module_path = tmp_path / MODULES / "empty_module"
        (module_path).mkdir(parents=True)
        (module_path / ".gitkeep").touch()
        modules, _ = Modules.load(tmp_path, selection=["empty_module"])
        assert len(modules.modules) == 0

    def test_module_container_with_resources_and_nested_module(self, tmp_path: Path) -> None:
        global_path = tmp_path / MODULES / "global"
        (global_path / "transformations").mkdir(parents=True)
        (global_path / "transformations" / "transformation.yaml").touch()

        # Create a nested module inside "global"
        nested_module_path = global_path / "nested_module"
        (nested_module_path / "transformations").mkdir(parents=True)
        (nested_module_path / "transformations" / "transformation.yaml").touch()

        modules, issues = Modules.load(tmp_path)

        module_paths = {module.path for module in modules.modules}

        # Only the deepest module should be loaded
        assert len(modules.modules) == 1
        assert nested_module_path in module_paths
        assert global_path not in module_paths

        # The parent module should be discarded with a ModuleLoadingIssue
        parent_issues = [
            issue for issue in issues if isinstance(issue, ModuleLoadingIssue) and issue.path == global_path
        ]
        assert len(parent_issues) > 0, (
            f"Expected a ModuleLoadingIssue for parent module {global_path}, but found issues: {issues}"
        )

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

        modules, issues = Modules.load(tmp_path)

        # The module should be loaded (functions with subfolders should still be detected)
        assert len(modules.modules) == 1
        assert modules.modules[0].path == module_path

        # No issues should be raised (functions folder should be recognized even with subfolders)
        assert len(issues) == 0, f"Expected no issues, but got: {issues}"
