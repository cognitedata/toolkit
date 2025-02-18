from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.data_classes import Environment
from tests.data import PROJECT_FOR_TEST


@pytest.fixture(scope="session")
def project_for_test_config_dev_yaml() -> str:
    return (PROJECT_FOR_TEST / "config.dev.yaml").read_text()


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        validation_type="dev",
        selected=["none"],
    )
