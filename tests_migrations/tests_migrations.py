import contextlib
import os
import platform
import shutil
import subprocess
from collections.abc import Iterable, Iterator
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("old_version_script_dir, old_version", list(cdf_tk_cmd_all_versions())[:1])
def tests_init_migrate_build_deploy(
    old_version_script_dir: Path, old_version: str, local_tmp_project_path: Path, local_build_path: Path
) -> str:
    modified_env_variables = os.environ.copy()
    repo_root = TEST_DIR_ROOT.parent
    # Need to remove the repo root from PYTHONPATH to avoid importing the wrong version of the toolkit
    modified_env_variables["PYTHONPATH"] = modified_env_variables["PYTHONPATH"].replace(str(repo_root), "")
    with chdir(TEST_DIR_ROOT):
        version_output = subprocess.run(
            [
                str(old_version_script_dir / "cdf-tk"),
                "--version",
            ],
            capture_output=True,
            shell=True,
            env=modified_env_variables,
        )
        stdout = version_output.stdout.decode("utf-8").strip()
        assert stdout.startswith(f"CDF-Toolkit version: {old_version}"), "Failed to setup the correct environment"
