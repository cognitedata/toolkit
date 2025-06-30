from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import typer
from _pytest.mark import ParameterSet
from _pytest.monkeypatch import MonkeyPatch
from questionary import Choice

from cognite_toolkit._cdf_tk.commands import BuildCommand, ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Package, Packages
from cognite_toolkit._cdf_tk.tk_warnings import DuplicatedItemWarning, TemplateVariableWarning
from tests.test_unit.utils import MockQuestionary


def get_packages() -> list[ParameterSet]:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    # - The Bootcamp package has intentionally warnings that is part of the learning experience.
    # - Examples and sourcesystems are tested separately, in that each example is tested individually as they
    # - Custom is just scaffolding and should never issue warnings.
    # should be independent of each other.
    packages = (
        package
        for package in packages.values()
        if package.name not in ["bootcamp", "sourcesystem", "industrial_tools", "contextualization", "custom"]
    )
    return [pytest.param(package, id=package.name) for package in sorted(packages, key=lambda p: p.name)]


@pytest.mark.parametrize("package", get_packages())
def test_build_packages_without_warnings(
    package: Package, tmp_path: Path, build_tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    organization_dir = tmp_path

    module_cmd = ModulesCommand(silent=True, skip_tracking=True)

    def select_package(choices: Sequence[Choice]) -> Any:
        return next(choice.value for choice in choices if choice.value.name == package.name)

    answers = [
        select_package,
        False,
        True,
        ["dev"],
    ]
    if package.can_cherry_pick:
        answers.insert(1, MockQuestionary.select_all)

    if package.name == "quickstart":
        # Skip downloading example data.
        answers.append(False)

    with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers), pytest.raises(typer.Exit) as exc_info:
        module_cmd.init(organization_dir, clean=True)

    assert exc_info.value.exit_code == 0

    build_cmd = BuildCommand(silent=True, skip_tracking=True)

    monkeypatch.setenv("CDF_PROJECT", "<my-project-dev>")
    build_cmd.execute(
        verbose=False,
        build_dir=build_tmp_path,
        organization_dir=organization_dir,
        build_env_name="dev",
        no_clean=False,
        client=None,
        selected=None,
    )

    # TemplateVariableWarning is when <change_me> is not replaced in the config file.
    # This is expected to be replaced by the users, and will thus raise when we run a fully automated test.
    warnings = [warning for warning in build_cmd.warning_list if not isinstance(warning, TemplateVariableWarning)]
    if package.name == "quickstart":
        # QuickStart combines modules that have the same spaces/datasets etc.
        warnings = [warning for warning in warnings if not isinstance(warning, DuplicatedItemWarning)]

    assert not warnings, f"{len(warnings)} warnings found: {warnings}"


def get_individual_modules() -> list[str]:
    packages = Packages.load(BUILTIN_MODULES_PATH)

    for package_name in ["sourcesystem"]:
        modules = packages[package_name]
        for module_name in sorted(modules.module_names):
            yield package_name, module_name


@pytest.mark.parametrize("package, module_name", get_individual_modules())
def test_build_individual_module(
    package: str, module_name: str, tmp_path: Path, build_tmp_path: Path, monkeypatch
) -> None:
    organization_dir = tmp_path

    module_cmd = ModulesCommand(silent=True, skip_tracking=True)

    def select_package(choices: Sequence[Choice]) -> Any:
        return next(choice.value for choice in choices if choice.value.name == package)

    def select_module(choices: Sequence[Choice]) -> Any:
        return [next(choice.value for choice in choices if choice.value.name == module_name)]

    answers = [
        select_package,
        select_module,
        False,
        True,
        ["dev"],
    ]
    if package == "sourcesystem":
        # Answer no to download example data.
        answers.append(False)

    with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers), pytest.raises(typer.Exit) as exc_info:
        module_cmd.init(organization_dir, clean=True)

    assert exc_info.value.exit_code == 0

    build_cmd = BuildCommand(silent=True, skip_tracking=True)

    monkeypatch.setenv("CDF_PROJECT", "<my-project-dev>")
    build_cmd.execute(
        verbose=False,
        build_dir=build_tmp_path,
        organization_dir=organization_dir,
        build_env_name="dev",
        no_clean=False,
        client=None,
        selected=None,
    )

    # TemplateVariableWarning is when <change_me> is not replaced in the config file.
    # This is expected to be replaced by the users, and will thus raise when we run a fully automated test.
    warnings = [warning for warning in build_cmd.warning_list if not isinstance(warning, TemplateVariableWarning)]

    assert not warnings, f"{len(warnings)} warnings found: {warnings}"


def test_all_modules_cdf_prefixed() -> None:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    missing_cdf_prefix = {
        module.name
        for package in packages.values()
        # Bootcamp has special structure
        if package.name not in {"bootcamp", "custom"}
        for module in package.modules
        if not module.name.startswith("cdf_")
    }

    assert not missing_cdf_prefix, f"Modules missing cdf_ prefix: {missing_cdf_prefix}"


def test_no_builtin_duplicates(organization_dir: Path, build_tmp_path: Path) -> None:
    cmd = BuildCommand(silent=True)

    modules = Path("modules")
    cmd.execute(
        verbose=False,
        organization_dir=organization_dir,
        build_dir=build_tmp_path,
        build_env_name="dev",
        no_clean=False,
        client=None,
        selected=[
            modules / "cdf_ingestion",
            modules / "common",
            modules / "contextualization",
            modules / "industrial_tools",
            modules / "models",
            modules / "sourcesystem",
        ],
    )

    duplicate_warning = [warning for warning in cmd.warning_list if isinstance(warning, DuplicatedItemWarning)]

    assert not duplicate_warning, f"{len(duplicate_warning)} duplicate warnings found: {duplicate_warning}"


def test_all_extra_resources_exists() -> None:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    missing_resources = {
        extra: module.name
        for package in packages.values()
        for module in package.modules
        if module.definition
        for extra in module.definition.extra_resources
        if not (BUILTIN_MODULES_PATH / extra).exists()
    }
    missing_by_module = {v: k.as_posix() for k, v in missing_resources.items()}

    assert not missing_resources, f"Modules missing resources: {missing_by_module}"
