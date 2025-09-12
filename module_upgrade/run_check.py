# ruff: noqa: E402
import contextlib
import logging
import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cognite.client.config import global_config

from cognite_toolkit._cdf_tk.utils.file import safe_rmtree
from tests.auth_utils import EnvironmentVariables

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml

# Hack to make the script work as running cdf modules upgrade
original_argv = sys.argv
sys.argv = ["cdf", "modules", "upgrade"]

import yaml
from dotenv import load_dotenv
from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand, ModulesCommand
from cognite_toolkit._cdf_tk.commands import _cli_commands as CLICommands
from cognite_toolkit._cdf_tk.commands._changes import ManualChange
from cognite_toolkit._cdf_tk.constants import ROOT_MODULES, SUPPORT_MODULE_UPGRADE_FROM_VERSION
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import module_from_path
from cognite_toolkit._version import __version__

TEST_DIR_ROOT = Path(__file__).resolve().parent
PROJECT_INIT_DIR = TEST_DIR_ROOT / "project_inits"
PROJECT_INIT_DIR.mkdir(exist_ok=True)

TODAY = date.today()

logging.basicConfig(
    filename=f"module_upgrade_{TODAY.strftime('%Y-%m-%d')}.log",
    filemode="w",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


def run() -> None:
    from_earliest = len(original_argv) > 1 and "--earliest" in original_argv[1:3]
    from_latest = len(original_argv) > 1 and "--latest" in original_argv[1:3]
    selected_version = len(original_argv) > 1 and "--version" in original_argv[1:3] and original_argv[2]

    versions = get_versions_since(SUPPORT_MODULE_UPGRADE_FROM_VERSION)
    if selected_version:
        selected = parse_version(selected_version)
        versions = [version for version in versions if version == selected]
        if not versions:
            raise ValueError(f"Version {selected_version} is not available.")
    elif from_earliest and from_latest:
        versions = [versions[0], versions[-1]]
    elif from_earliest:
        versions = versions[-1:]
    elif from_latest:
        versions = versions[:1]

    for version in versions:
        create_project_init(str(version))

    print(
        Panel(
            "All projects inits created successfully.",
            expand=False,
            title="cdf-tk init executed for all past versions.",
        )
    )

    print(
        Panel(
            "Running module upgrade for all supported versions.",
            expand=False,
            title="cdf-tk module upgrade",
        )
    )
    for version in versions:
        with (
            local_tmp_project_path() as project_path,
            local_build_path() as build_path,
            get_env_vars() as env_vars,
        ):
            run_modules_upgrade(version, project_path, build_path, env_vars)


def get_versions_since(support_upgrade_from_version: str) -> list[Version]:
    result = subprocess.run("pip index versions cognite-toolkit --pre".split(), stdout=subprocess.PIPE)
    lines = result.stdout.decode().split("\n")
    for line in lines:
        if line.startswith("Available versions:"):
            raw_version_str = line.split(":", maxsplit=1)[1]
            supported_from = parse_version(support_upgrade_from_version)
            return [
                parsed
                for version in raw_version_str.split(",")
                if (parsed := parse_version(version.strip())) >= supported_from
            ]
    else:
        raise ValueError("Could not find available versions.")


def create_project_init(version: str) -> None:
    project_init = PROJECT_INIT_DIR / f"project_{version}"
    if project_init.exists():
        print(f"Project init for version {version} already exists.")
        return

    environment_directory = f".venv{version}"
    if (TEST_DIR_ROOT / environment_directory).exists():
        print(f"Environment for version {version} already exists")
    else:
        with chdir(TEST_DIR_ROOT):
            print(f"Creating environment for version {version}")
            create_venv = subprocess.run(["python", "-m", "venv", environment_directory])
            if create_venv.returncode != 0:
                raise ValueError(f"Failed to create environment for version {version}")

            if platform.system() == "Windows":
                install_toolkit = subprocess.run(
                    [f"{environment_directory}/Scripts/pip", "install", f"cognite-toolkit=={version}"]
                )
            else:
                install_toolkit = subprocess.run(
                    [f"{environment_directory}/bin/pip", "install", f"cognite-toolkit=={version}"]
                )

            if install_toolkit.returncode != 0:
                raise ValueError(f"Failed to install toolkit version {version}")

            if parse_version(version) < parse_version("0.2.4"):
                # Bug in pre 0.2.4 versions that was missing the packaging dependency
                if platform.system() == "Windows":
                    install_packing = subprocess.run(
                        [f"{environment_directory}/Scripts/pip", "install", "packaging==24.1"]
                    )
                else:
                    install_packing = subprocess.run([f"{environment_directory}/bin/pip", "install", "packaging==24.1"])

                if install_packing.returncode != 0:
                    raise ValueError(f"Failed to install packing version {version}")

            print(f"Environment for version {version} created")

    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    if "PYTHONPATH" in modified_env_variables:
        # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
        # (This is typically set by the IDE, for example, PyCharm sets it when running tests).
        modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")
    if platform.system() == "Windows":
        old_version_script_dir = Path(f"{environment_directory}/Scripts/")
    else:
        old_version_script_dir = Path(f"{environment_directory}/bin/")
    with chdir(TEST_DIR_ROOT):
        version_parsed = parse_version(version)
        if version_parsed >= parse_version("0.3.0a1"):
            cmd = [
                str(old_version_script_dir / "cdf"),
                "modules",
                "init",
                f"{PROJECT_INIT_DIR.name}/{project_init.name}",
                "--clean",
                "--all",
            ]
        else:
            cmd = [
                str(old_version_script_dir / "cdf-tk"),
                "init",
                f"{PROJECT_INIT_DIR.name}/{project_init.name}",
            ]

        output = subprocess.run(
            cmd,
            capture_output=True,
            shell=True if platform.system() == "Windows" else False,
            env=modified_env_variables,
        )
        if output.returncode != 0:
            print(output.stderr.decode())
            raise ValueError(f"Failed to create project init for version {version}.")

        cdf_toml_path = TEST_DIR_ROOT / CDFToml.file_name
        if cdf_toml_path.exists():
            shutil.move(cdf_toml_path, project_init / CDFToml.file_name)

    print(f"Project init for version {version} created.")
    with chdir(TEST_DIR_ROOT):
        safe_rmtree(environment_directory)


def run_modules_upgrade(
    previous_version: Version, project_path: Path, build_path: Path, env_vars: EnvironmentVariables
) -> None:
    if (TEST_DIR_ROOT / CDFToml.file_name).exists():
        # Cleanup after previous run
        (TEST_DIR_ROOT / CDFToml.file_name).unlink()

    project_init = PROJECT_INIT_DIR / f"project_{previous_version!s}"
    # Copy the project to a temporary location as the upgrade command modifies the project.
    shutil.copytree(project_init, project_path, dirs_exist_ok=True)
    if previous_version >= parse_version("0.3.0a1"):
        # Move out the CDF.toml file to use
        shutil.move(project_path / CDFToml.file_name, TEST_DIR_ROOT / CDFToml.file_name)

    with chdir(TEST_DIR_ROOT):
        modules = ModulesCommand(print_warning=False)
        # This is to allow running the function with having uncommitted changes in the repository.
        with patch.object(CLICommands, "has_uncommitted_changes", lambda: False):
            changes = modules.upgrade(project_path)
        logging.info(f"Changes for version {previous_version!s} to {__version__}: {len(changes)}")
        delete_modules_requiring_manual_changes(changes)

        # Update the config file to run include all modules.
        update_config_yaml_to_select_all_modules(project_path)

        if previous_version < parse_version("0.2.0a4"):
            # Bug in pre 0.2.0a4 versions
            pump_view = (
                project_path
                / "cognite_modules"
                / "experimental"
                / "example_pump_data_model"
                / "data_models"
                / "4.Pump.view.yaml"
            )
            if pump_view.exists():
                pump_view.write_text(pump_view.read_text().replace("external_id", "externalId"))

        build = BuildCommand(print_warning=False)
        build.execute(False, project_path, build_path, selected=None, build_env_name="dev", no_clean=False)

        deploy = DeployCommand(print_warning=False)
        deploy.deploy_build_directory(
            env_vars,
            build_path,
            build_env_name="dev",
            dry_run=True,
            drop=False,
            drop_data=False,
            force_update=False,
            include=list(CRUDS_BY_FOLDER_NAME),
            verbose=False,
        )

    print(
        Panel(
            f"Module upgrade for version {previous_version!s} to {__version__} completed successfully.",
            expand=False,
            style="green",
        )
    )
    logging.info(f"Module upgrade for version {previous_version!s} to {__version__} completed successfully.")


def delete_modules_requiring_manual_changes(changes):
    for change in changes:
        if not isinstance(change, ManualChange):
            continue
        for file in change.needs_to_change():
            if file.is_dir():
                safe_rmtree(file)
            else:
                module = module_from_path(file)
                for part in reversed(file.parts):
                    if part == module:
                        break
                    file = file.parent
                if file.exists():
                    safe_rmtree(file)


def update_config_yaml_to_select_all_modules(project_path):
    config_yaml = project_path / "config.dev.yaml"
    assert config_yaml.exists()
    yaml_data = yaml.safe_load(config_yaml.read_text())
    yaml_data["environment"]["selected"] = []
    for root_module in ROOT_MODULES:
        if (project_path / root_module).exists() and any(
            yaml_file for yaml_file in (project_path / root_module).rglob("*.yaml")
        ):
            yaml_data["environment"]["selected"].append(f"{root_module}/")
    config_yaml.write_text(yaml.dump(yaml_data))


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


@contextmanager
def get_env_vars() -> Iterator[EnvironmentVariables]:
    load_dotenv(TEST_DIR_ROOT.parent / ".env")

    try:
        yield EnvironmentVariables.create_from_environ()
    finally:
        ...


@contextmanager
def local_tmp_project_path() -> Path:
    project_path = TEST_DIR_ROOT / "tmp-project"
    if project_path.exists():
        safe_rmtree(project_path)
    project_path.mkdir(exist_ok=True)
    try:
        yield project_path
    finally:
        ...


@contextmanager
def local_build_path() -> Path:
    build_path = TEST_DIR_ROOT / "build"
    if build_path.exists():
        safe_rmtree(build_path)

    build_path.mkdir(exist_ok=True)
    # This is a small hack to get 0.1.0b1-4 working
    (build_path / "file.txt").touch(exist_ok=True)
    try:
        yield build_path
    finally:
        ...


if __name__ == "__main__":
    run()
