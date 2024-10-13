from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import BuildCommand, ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, ModuleDirectories, Packages
from cognite_toolkit._cdf_tk.tk_warnings import TemplateVariableWarning


def get_packages() -> list[str]:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    return list(packages.keys())


@pytest.mark.parametrize("package", get_packages())
def test_build_packages_without_warnings(package: str, tmp_path: Path, build_tmp_path: Path) -> None:
    assert package
    organization_dir = tmp_path

    module_cmd = ModulesCommand(silent=True, skip_tracking=True)
    module_cmd.init(organization_dir, clean=True)

    build_cmd = BuildCommand(silent=True, skip_tracking=True)

    modules = ModuleDirectories.load(organization_dir)
    config = BuildConfigYAML.load_default(organization_dir)
    variables = BuildVariables.load_raw(config.variables, modules.available_paths, modules.selected.available_paths)
    build_cmd.build_modules(modules, build_tmp_path, variables, verbose=False, progress_bar=False)

    # TemplateVariableWarning is when <change_me> is not replaced in the config file.
    # This is expected to be replaced by the users, and will thus raise when we run a fully automated test.
    warnings = [warning for warning in build_cmd.warning_list if not isinstance(warning, TemplateVariableWarning)]

    assert not warnings, f"Warnings found: {warnings}"
