from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.signal_subscription import SignalSubscriptionYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "sub-1", "sink": {"type": "email", "externalId": "s1"}},
        {"Missing required field: 'filter'"},
        id="missing-filter",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "filter": {"topic": "cognite_workflows"},
        },
        {"Missing required field: 'sink'"},
        id="missing-sink",
    )
    yield pytest.param(
        {
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "cognite_workflows"},
        },
        {"Missing required field: 'externalId'"},
        id="missing-external-id",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "invalid_topic"},
        },
        {
            "In field filter input tag 'invalid_topic' found using 'topic' "
            "does not match any of the expected tags: 'cognite_integrations', 'cognite_workflows'",
        },
        id="invalid-topic",
    )
    yield pytest.param(
        {
            "externalId": "sub-1",
            "sink": {"type": "email", "externalId": "s1"},
            "filter": {"topic": "cognite_integrations"},
            "unknownField": "x",
        },
        {"Unknown field: 'unknownField'"},
        id="unknown-field",
    )


class TestSignalSubscriptionYAML:
    @pytest.mark.parametrize("data", list(find_resources("Subscription", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid_subscription(self, data: dict[str, object]) -> None:
        loaded = SignalSubscriptionYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_subscription_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SignalSubscriptionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
