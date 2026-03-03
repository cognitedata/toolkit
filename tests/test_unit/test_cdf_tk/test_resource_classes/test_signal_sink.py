from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.user_profile import UserProfile
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._resource_cruds.signal_sink import SignalSinkCRUD
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
        {"Invalid signal sink type 'invalid'. Expected one of email or user"},
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
        loaded = SignalSinkYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_email_sink_returns_correct_subclass(self) -> None:
        loaded = SignalSinkYAML.model_validate({"type": "email", "externalId": "s1", "emailAddress": "a@b.com"})
        assert isinstance(loaded, EmailSinkYAML)
        assert loaded.email_address == "a@b.com"

    def test_user_sink_returns_correct_subclass(self) -> None:
        loaded = SignalSinkYAML.model_validate({"type": "user", "externalId": "s2"})
        assert isinstance(loaded, UserSinkYAML)

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_sink_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SignalSinkYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors


def _make_user_profile(email: str) -> UserProfile:
    return UserProfile(
        user_identifier=f"user-{email}",
        email=email,
        identity_type="USER",
        last_updated_time=0,
    )


class TestSignalSinkCRUDEmailWarning:
    @pytest.fixture
    def mock_client(self) -> ToolkitClient:
        client = create_autospec(ToolkitClient, instance=True)
        client.user_profiles = MagicMock()
        client.user_profiles.list.return_value = [
            _make_user_profile("known-user@example.com"),
        ]
        return client

    @pytest.fixture
    def crud(self, mock_client: ToolkitClient) -> SignalSinkCRUD:
        return SignalSinkCRUD(mock_client, build_dir=None)

    def test_known_email_no_warning(self, crud: SignalSinkCRUD, capsys: pytest.CaptureFixture[str]) -> None:
        crud.load_resource(
            {"type": "email", "externalId": "sink-ok", "emailAddress": "known-user@example.com"},
            is_dry_run=True,
        )
        assert "does not match" not in capsys.readouterr().out

    def test_unknown_email_warns(self, crud: SignalSinkCRUD, capsys: pytest.CaptureFixture[str]) -> None:
        crud.load_resource(
            {"type": "email", "externalId": "sink-bad", "emailAddress": "nobody@nonexistent.invalid"},
            is_dry_run=True,
        )
        captured = capsys.readouterr().out
        assert "nobody@nonexistent.invalid" in captured
        assert "does not match any known user profile" in captured

    def test_user_type_skips_email_check(self, crud: SignalSinkCRUD) -> None:
        result = crud.load_resource(
            {"type": "user", "externalId": "sink-user"},
            is_dry_run=True,
        )
        assert result.type == "user"
