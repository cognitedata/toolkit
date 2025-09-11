from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.hosted_extractor_source import HostedExtractorSourceYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_hosted_extractor_source_test_cases() -> Iterable:
    # Invalid type
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "invalid",
            "host": "http://example.com",
            "published": True,
            "keyName": "apiKey",
            "keyValue": "secret",
        },
        {
            "Invalid hosted extractor source type='invalid'. Expected one of eventhub, kafka, mqtt3, mqtt5 or rest",
        },
        id="Invalid source type",
    )
    # Invalid eventhub (missing required event_hub_name)
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "eventhub",
            "host": "host",
            "keyName": "key",
            "keyValue": "secret",
            # missing eventHubName
        },
        {
            "Missing required field: 'eventHubName'",
        },
        id="EventHubSource missing event_hub_name",
    )
    # Invalid kafka (missing bootstrap_brokers)
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "kafka",
            # missing bootstrapBrokers
        },
        {
            "Missing required field: 'bootstrapBrokers'",
        },
        id="KafkaSource missing bootstrap_brokers",
    )
    # Invalid rest (missing host)
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "rest",
            # missing host
        },
        {
            "Missing required field: 'host'",
        },
        id="RESTSource missing host",
    )
    # Invalid mqtt3 (missing host)
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "mqtt3",
            # missing host
        },
        {
            "Missing required field: 'host'",
        },
        id="MQTT3Source missing host",
    )
    # Invalid mqtt5 (missing host)
    yield pytest.param(
        {
            "externalId": "mySource",
            "type": "mqtt5",
            # missing host
        },
        {
            "Missing required field: 'host'",
        },
        id="MQTT5Source missing host",
    )


class TestHostedExtractorSourceYAML:
    @pytest.mark.parametrize("data", list(find_resources("Source", resource_dir="hosted_extractors")))
    def test_load_valid_hosted_extractor_source(self, data: dict[str, object]) -> None:
        loaded = HostedExtractorSourceYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True, mode="json") == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_hosted_extractor_source_test_cases()))
    def test_invalid_hosted_extractor_source_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, HostedExtractorSourceYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
