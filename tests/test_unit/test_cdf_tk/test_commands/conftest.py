from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES


@pytest.fixture
def project_statistics_response() -> dict[str, Any]:
    """Minimal valid DMS project statistics response (incl. soft-delete fields)."""
    return {
        "spaces": {"count": 5, "limit": 100},
        "containers": {"count": 42, "limit": 1000},
        "views": {"count": 123, "limit": 2000},
        "dataModels": {"count": 8, "limit": 500},
        "containerProperties": {"count": 1234, "limit": 100},
        "instances": {
            "edges": 5000,
            "softDeletedEdges": 100,
            "nodes": 10000,
            "softDeletedNodes": 200,
            "instances": 15000,
            "instancesLimit": 5000000,
            "softDeletedInstances": 300,
            "softDeletedInstancesLimit": 10000000,
        },
        "concurrentReadLimit": 10,
        "concurrentWriteLimit": 5,
        "concurrentDeleteLimit": 3,
    }


@pytest.fixture()
def valid_yaml_absolute_path(tmp_path: Path) -> Path:
    """Fixture to provide a valid absolute path for testing."""
    valid_path = tmp_path / MODULES / "valid.yaml"
    valid_path.parent.mkdir(parents=True, exist_ok=True)
    valid_path.touch()
    return valid_path
