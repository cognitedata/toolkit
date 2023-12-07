from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit.cdf_tk.templates import create_local_config, generate_config, split_config

BUILD_CONFIG = Path(__file__).parent / "build_configs"


def generate_config_test_cases():
    expected = {
        "a_module": {
            "readwrite_source_id": "<change_me>",
            "readonly_source_id": "<change_me>",
        },
        "another_module": {
            "default_location": "oid",
            "source_asset": "workmate",
            "source_workorder": "workmate",
            "source_files": "fileshare",
            "source_timeseries": "pi",
        },
        "parent_module": {"child_module": {"child_variable": "<change_me>"}},
    }
    yield pytest.param(expected, None, id="Include all")

    only_a_module = {
        "a_module": {
            "readwrite_source_id": "<change_me>",
            "readonly_source_id": "<change_me>",
        }
    }
    yield pytest.param(only_a_module, {"a_module"}, id="Include one module")


@pytest.mark.parametrize(
    "expected, include",
    list(generate_config_test_cases()),
)
def test_generate_config(expected, include: set[str] | None) -> None:
    actual = generate_config(BUILD_CONFIG, include_modules=include)

    assert actual == expected


@pytest.fixture()
def my_config():
    return {
        "top_variable": "my_top_variable",
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent": {"child": {"child_variable": "my_child_variable"}},
    }


def test_split_config(my_config: dict[str, Any]) -> None:
    expected = {
        "": {"top_variable": "my_top_variable"},
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent.child": {"child_variable": "my_child_variable"},
    }
    actual = split_config(my_config)

    assert actual == expected


def test_create_local_config(my_config: dict[str, Any]):
    configs = split_config(my_config)

    local_config = create_local_config(configs, Path("parent/child/auth/"))

    assert dict(local_config.items()) == {"top_variable": "my_top_variable", "child_variable": "my_child_variable"}
