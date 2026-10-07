from collections.abc import Iterable
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.signal_sink import EmailSinkYAML, SignalSinkYAML, UserSinkYAML
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
