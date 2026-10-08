from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.signal_sink import EmailSinkYAML, SignalSink, SignalSinkYAML, UserSinkYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"type": "email", "externalId": "my-sink"},
        {"Missing required field: 'emailAddress'"},
        id="email-type-missing-email-address",
    )
    yield pytest.param(
        {"externalId": "my-sink"},
        {"Missing required field: 'type'"},
        id="missing-required-field-type",
    )
    yield pytest.param(
        {"type": "email", "externalId": "my-sink", "emailAddress": "a@b.com", "unknownField": "x"},
        {"Unknown field: 'unknownField'"},
        id="unknown-field",
    )
    yield pytest.param(
        {"type": "invalid", "externalId": "my-sink"},
        {"Input tag 'invalid' found using 'type' does not match any of the expected tags: 'email', 'user'"},
        id="invalid-type",
    )
    yield pytest.param(
        {"type": "user", "externalId": "my-sink", "emailAddress": "a@b.com"},
        {"Unknown field: 'emailAddress'"},
        id="user-type-rejects-email-address",
    )


class TestSignalSinkYAML:
    @pytest.mark.parametrize("data", list(find_resources("Sink", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid_sink(self, data: dict[str, object]) -> None:
        loaded = TypeAdapter(SignalSinkYAML).validate_python(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_email_sink_returns_correct_subclass(self) -> None:
        loaded = TypeAdapter(SignalSinkYAML).validate_python(
            {"type": "email", "externalId": "s1", "emailAddress": "a@b.com"}
        )
        assert isinstance(loaded, EmailSinkYAML)
        assert loaded.email_address == "a@b.com"

    def test_user_sink_returns_correct_subclass(self) -> None:
        loaded = TypeAdapter(SignalSinkYAML).validate_python({"type": "user", "externalId": "s2"})
        assert isinstance(loaded, UserSinkYAML)

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_sink_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SignalSinkYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors

    def test_all_sinks_in_union(self) -> None:
        """Test that all sink types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(SignalSink))
        subclasses = set(get_args(SignalSinkYAML.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"
