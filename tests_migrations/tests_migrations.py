import contextlib
import os
import platform
import shutil
import subprocess
from collections.abc import Iterable, Iterator
from pathlib import Path

import pytest

from cognite_toolkit._version import __version__
from tests_migrations.constants import SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT


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


def cdf_tk_cmd_all_versions() -> Iterable[tuple[Path, str]]:
    for version in SUPPORTED_TOOLKIT_VERSIONS:
        environment_directory = f".venv{version}"
        if (TEST_DIR_ROOT / environment_directory).exists():
            if platform.system() == "Windows":
                yield pytest.param(Path(f"{environment_directory}/Scripts/"), version, id=f"cdf-tk-{version}")
            else:
                yield pytest.param(Path(f"{environment_directory}/bin/"), version, id=f"cdf-tk-{version}")
        else:
            pytest.skip("Environment for version {version} does not exist, run 'create_environments.py' to create it.")


@pytest.fixture(scope="function")
def local_tmp_project_path() -> Path:
    project_path = TEST_DIR_ROOT / "tmp-project"
    if project_path.exists():
        shutil.rmtree(project_path)
    project_path.mkdir(exist_ok=True)
    yield project_path


@pytest.fixture(scope="function")
def local_build_path() -> Path:
    build_path = TEST_DIR_ROOT / "build"
    if build_path.exists():
        shutil.rmtree(build_path)
    build_path.mkdir(exist_ok=True)
    yield build_path


@pytest.mark.parametrize("old_version_script_dir, old_version", list(cdf_tk_cmd_all_versions()))
def tests_init_migrate_build_deploy(
    old_version_script_dir: Path, old_version: str, local_tmp_project_path: Path, local_build_path: Path
) -> None:
    project_name = local_tmp_project_path.name
    build_name = local_build_path.name

    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
    modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")
    old_command = str(old_version_script_dir / "cdf-tk")

    with chdir(TEST_DIR_ROOT):
        version_output = subprocess.run(
            [
                old_command,
                "--version",
            ],
            capture_output=True,
            shell=True,
            env=modified_env_variables,
        )
        stdout = version_output.stdout.decode("utf-8").strip()
        assert version_output.returncode == 0, f"Failed to run {old_command}: {version_output.stderr.decode('utf-8')}"
        assert stdout.startswith(
            f"CDF-Toolkit version: {old_version}"
        ), f"Failed to setup the correct environment for {old_version}"

        init_output = subprocess.run(
            [old_command, "init", project_name, "--clean"],
            capture_output=True,
            env=modified_env_variables,
            shell=True,
        )
        assert (
            init_output.returncode == 0
        ), f"Failed to init project with {old_version}: {init_output.stderr.decode('utf-8')}"
        build_output = subprocess.run(
            [old_command, "build", project_name, "--env", "dev"],
            capture_output=True,
            env=modified_env_variables,
            shell=True,
        )
        assert (
            build_output.returncode == 0
        ), f"Failed to build project with {old_version}: {build_output.stderr.decode('utf-8')}"
        previous_output = subprocess.run(
            [old_command, "deploy", "--env", "dev", "--dry-run"], capture_output=True, env=modified_env_variables
        )
        assert (
            previous_output.returncode == 0
        ), f"Failed to deploy project with {old_version}: {previous_output.stderr.decode('utf-8')}"

        current_version = subprocess.run(
            [
                "cdf-tk",
                "--version",
            ],
            capture_output=True,
            shell=True,
        )
        stdout = current_version.stdout.decode("utf-8").strip()
        assert current_version.returncode == 0, f"Failed to run cdf-tk: {current_version.stderr.decode('utf-8')}"
        assert stdout.startswith(
            f"CDF-Toolkit version: {__version__}"
        ), "Failed to setup the correct environment for the current version"
        current_build = subprocess.run(
            ["cdf-tk", "build", "--env", "dev", "--build-dir", build_name, "--clean"], capture_output=True, shell=True
        )
        assert (
            current_build.returncode == 0
        ), f"Failed to build project with current version: {current_build.stderr.decode('utf-8')}"
        current_output = subprocess.run(
            ["cdf-tk", "deploy", "--env", "dev", "--dry-run"], capture_output=True, shell=True
        )
        assert (
            current_output.returncode == 0
        ), f"Failed to deploy project with current version: {current_output.stderr.decode('utf-8')}"
