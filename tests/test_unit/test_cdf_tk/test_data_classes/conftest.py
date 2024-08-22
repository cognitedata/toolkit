from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.data_classes import Environment
from tests.data import PROJECT_FOR_TEST


@pytest.fixture(scope="session")
def config_yaml() -> str:
    return (PROJECT_FOR_TEST / "config.dev.yaml").read_text()


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )
