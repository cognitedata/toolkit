"""In this test module, we are testing that the implementations of warnings consistently implemented."""

from abc import ABC
from typing import TypeVar

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning

T_Type = TypeVar("T_Type", bound=type)


def get_all_subclasses(cls: T_Type, only_concrete: bool = False) -> list[T_Type]:
    """Get all subclasses of a class."""
    return [s for s in cls.__subclasses__() if only_concrete is False or ABC not in s.__bases__] + [
        g for s in cls.__subclasses__() for g in get_all_subclasses(s, only_concrete)
    ]


def test_warnings_class_names_suffix_warning() -> None:
    """Test that all classes that inherit from ValidationWarning have the suffix 'Warning'."""
    warnings = get_all_subclasses(ToolkitWarning)

    not_warning_suffix = [warning for warning in warnings if not warning.__name__.endswith("Warning")]

    assert not_warning_suffix == [], f"Warnings without 'Warning' suffix: {not_warning_suffix}"
