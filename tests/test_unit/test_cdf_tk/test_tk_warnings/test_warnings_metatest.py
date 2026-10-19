"""Invalid value for this: Test module, we are testing that the implementations of warnings consistently implemented."""

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


def test_warnings_class_names_suffix_warning() -> None:
    """Test that all classes that inherit from ValidationWarning have the suffix 'Warning'."""
    warnings = get_concrete_subclasses(ToolkitWarning)

    not_warning_suffix = [warning for warning in warnings if not warning.__name__.endswith("Warning")]

    assert not_warning_suffix == [], f"Warnings without 'Warning' suffix: {not_warning_suffix}"
