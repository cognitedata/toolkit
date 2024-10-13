from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands import BuildCommand, ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, ModuleDirectories, Packages
from cognite_toolkit._cdf_tk.tk_warnings import TemplateVariableWarning


class MockQuestion:
    def __init__(self, answer: Any) -> None:
        self.answer = answer

    def ask(self) -> Any:
        return self.answer


class MockQuestionary:
    def __init__(self, module_target: str, monkeypatch: MonkeyPatch, answers: Sequence[Any]) -> None:
        self.answers = answers
        for method in [self.select, self.confirm, self.checkbox, self.text]:
            monkeypatch.setattr(f"{module_target}.questionary.{method.__name__}", method)

    def select(self, *args, **kwargs) -> MockQuestion:
        raise NotImplementedError("select not implemented in MockQuestionary")

    def confirm(self, *args, **kwargs) -> MockQuestion:
        raise NotImplementedError("confirm not implemented in MockQuestionary")

    def checkbox(self, *args, **kwargs) -> MockQuestion:
        raise NotImplementedError("checkbox not implemented in MockQuestionary")

    def text(self, *args, **kwargs) -> MockQuestion:
        raise NotImplementedError("text not implemented in MockQuestionary")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        raise NotImplementedError("exit not implemented in MockQuestionary")


def get_packages() -> list[str]:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    return list(packages.keys())


@pytest.mark.parametrize("package", get_packages())
def test_build_packages_without_warnings(
    package: str, tmp_path: Path, build_tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    assert package

    organization_dir = tmp_path

    module_cmd = ModulesCommand(silent=True, skip_tracking=True)

    with MockQuestionary(ModulesCommand.__module__, monkeypatch, ["y"]):
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
