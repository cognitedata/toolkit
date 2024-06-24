from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.data_classes import Environment
from tests.data import PYTEST_PROJECT


@pytest.fixture(scope="session")
def config_yaml() -> str:
    return (PYTEST_PROJECT / "config.dev.yaml").read_text()


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )
