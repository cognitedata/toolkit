from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
import typer
from _pytest.monkeypatch import MonkeyPatch
from questionary import Choice

from cognite_toolkit._cdf_tk.commands import BuildCommand, ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Packages
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning, TemplateVariableWarning


class MockQuestion:
    def __init__(self, answer: Any, choices: list[Choice] | None = None) -> None:
        self.answer = answer
        self.choices = choices

    def ask(self) -> Any:
        if isinstance(self.answer, Callable):
            return self.answer(self.choices)
        return self.answer


class MockQuestionary:
    def __init__(self, module_target: str, monkeypatch: MonkeyPatch, answers: list[Any]) -> None:
        self.module_target = module_target
        self.answers = answers
        self.monkeypatch = monkeypatch

    def select(self, *_, choices: list[Choice], **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0), choices)

    def confirm(self, *_, **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0))

    def checkbox(self, *_, choices: list[Choice], **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0), choices)

    def text(self, *_, **__) -> MockQuestion:
        return MockQuestion(self.answers.pop(0))

    def __enter__(self):
        for method in [self.select, self.confirm, self.checkbox, self.text]:
            self.monkeypatch.setattr(f"{self.module_target}.questionary.{method.__name__}", method)
        return self

    def __exit__(self, *args):
        self.monkeypatch.undo()
        return False

    @staticmethod
    def select_all(choices: list[Choice]) -> list[str]:
        return [choice.value for choice in choices]


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

    def select_package(choices: Sequence[Choice]) -> Any:
        return next(choice.value for choice in choices if choice.value.name == package)

    answers = [
        select_package,
        MockQuestionary.select_all,
        False,
        True,
        ["dev"],
    ]
    with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers), pytest.raises(typer.Exit) as exc_info:
        module_cmd.init(organization_dir, clean=True)

    assert exc_info.value.exit_code == 0

    build_cmd = BuildCommand(silent=True, skip_tracking=True)

    build_cmd.execute(
        verbose=False,
        build_dir=build_tmp_path,
        organization_dir=organization_dir,
        build_env_name="dev",
        no_clean=False,
        ToolGlobals=None,
        selected=None,
    )

    # TemplateVariableWarning is when <change_me> is not replaced in the config file.
    # This is expected to be replaced by the users, and will thus raise when we run a fully automated test.
    warnings = [
        warning
        for warning in build_cmd.warning_list
        if not isinstance(warning, TemplateVariableWarning)
        and not (
            # This warning is expected to be raised when the 'CDF_PROJECT' environment variable is not set.
            isinstance(warning, MediumSeverityWarning)
            and warning.message_raw
            and warning.message_raw.startswith("No 'CDF_PROJECT' environment variable set.")
        )
    ]

    assert not warnings, f"{len(warnings)} warnings found: {warnings}"
