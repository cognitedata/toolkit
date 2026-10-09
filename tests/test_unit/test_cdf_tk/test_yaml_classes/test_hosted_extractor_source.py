from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.hosted_extractor_source import (
    Authentication,
    HeaderCredentials,
    HostedExtractorSource,
    HostedExtractorSourceYAML,
    KafkaAuthentication,
    QueryCredentials,
    RESTAuthentication,
    ScramSha256,
    ScramSha512,
)
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
            "Input tag 'invalid' found using 'type' does not match any of the expected "
            "tags: 'eventhub', 'rest', 'mqtt3', 'mqtt5', 'kafka', 'mqtt_broker'"
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
    # RESTSource with BasicAuthentication (missing password)
    yield pytest.param(
        {
            "externalId": "restBasicMissingPassword",
            "type": "rest",
            "host": "api.example.com",
            "authentication": {
                "type": "basic",
                "username": "user",
                # missing password
            },
        },
        {"Missing required field in authentication: 'password'"},
        id="RESTSource BasicAuthentication missing password",
    )
    # RESTSource with ClientCredentials (missing client_secret)
    yield pytest.param(
        {
            "externalId": "restClientCredMissingSecret",
            "type": "rest",
            "host": "api.example.com",
            "authentication": {
                "type": "clientCredentials",
                "client_id": "id",
                "token_url": "https://token.url",
                "scopes": "scope",
                # missing client_secret
            },
        },
        {
            "Missing required fields in authentication: 'clientId', 'clientSecret' and 'tokenUrl'",
            "Unrecognized fields in authentication: 'client_id' and 'token_url'. ",
        },
        id="RESTSource ClientCredentials missing client_secret",
    )
    # RESTSource with QueryCredentials (missing value)
    yield pytest.param(
        {
            "externalId": "restQueryCredMissingValue",
            "type": "rest",
            "host": "api.example.com",
            "authentication": {
                "type": "query",
                "key": "api_key",
                # missing value
            },
        },
        {"Missing required field in authentication: 'value'"},
        id="RESTSource QueryCredentials missing value",
    )
    # RESTSource with HeaderCredentials (missing value)
    yield pytest.param(
        {
            "externalId": "restHeaderCredMissingValue",
            "type": "rest",
            "host": "api.example.com",
            "authentication": {
                "type": "header",
                "key": "Authorization",
                # missing value
            },
        },
        {"Missing required field in authentication: 'value'"},
        id="RESTSource HeaderCredentials missing value",
    )
    # RESTSource with ScramSha256 (invalid type)
    # RESTSource with ScramSha256 (invalid type)
    yield pytest.param(
        {
            "externalId": "restSourceWithInvalidAuth",
            "type": "rest",
            "host": "api.example.com",
            "authentication": {
                "type": "scramSha256",
                "username": "user",
                "password": "secret",
            },
        },
        {
            "Invalid value for authentication: Input tag 'scramSha256' found using 'type' does not match any of "
            "the expected tags: 'basic', 'clientCredentials', 'query', 'header'"
        },
        id="RESTSource with invalid auth type",
    )
    # KafkaSource with ScramSha256 (missing password)
    yield pytest.param(
        {
            "externalId": "kafkaScram256MissingPassword",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker", "port": 9092}],
            "authentication": {
                "type": "scramSha256",
                "username": "user",
                # missing password
            },
        },
        {"Missing required field in authentication: 'password'"},
        id="KafkaSource ScramSha256 missing password",
    )
    # KafkaSource with BasicAuthentication (missing password)
    yield pytest.param(
        {
            "externalId": "kafkaBasicMissingPassword",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker", "port": 9092}],
            "authentication": {
                "type": "basic",
                "username": "user",
                # missing password
            },
        },
        {"Missing required field in authentication: 'password'"},
        id="KafkaSource BasicAuthentication missing password",
    )
    # KafkaSource with ClientCredentials (missing client_secret)
    yield pytest.param(
        {
            "externalId": "kafkaClientCredMissingSecret",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker", "port": 9092}],
            "authentication": {
                "type": "clientCredentials",
                "clientId": "id",
                "tokenUrl": "https://token.url",
                "scopes": "scope",
                # missing client_secret
            },
        },
        {"Missing required field in authentication: 'clientSecret'"},
        id="KafkaSource ClientCredentials missing client_secret",
    )
    # KafkaSource with QueryCredentials (invalid type)
    yield pytest.param(
        {
            "externalId": "kafkaQueryCredMissingValue",
            "type": "kafka",
            "bootstrapBrokers": [{"host": "broker", "port": 9092}],
            "authentication": {
                "type": "query",
                "key": "api_key",
                "value": "api_secret",
            },
        },
        {
            "Invalid value for authentication: Input tag 'query' found using 'type' does not match any of "
            "the expected tags: 'basic', 'clientCredentials', 'scramSha256', 'scramSha512'"
        },
        id="KafkaSource QueryCredentials invalid type for Kafka",
    )


class TestHostedExtractorSourceYAML:
    @pytest.mark.parametrize("data", list(find_resources("Source", resource_dir="hosted_extractors")))
    def test_load_valid_hosted_extractor_source(self, data: dict[str, object]) -> None:
        loaded = TypeAdapter(HostedExtractorSourceYAML).validate_python(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True, mode="json") == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_hosted_extractor_source_test_cases()))
    def test_invalid_hosted_extractor_source_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, HostedExtractorSourceYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    def test_all_sources_in_union(self) -> None:
        """Test that all hosted extractor source types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(HostedExtractorSource))
        subclasses = set(get_args(HostedExtractorSourceYAML.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"

    def test_rest_authentication_union(self) -> None:
        """Scram authentication is valid for Kafka, and is excluded from REST authentication."""
        expected_subclasses = set(get_concrete_subclasses(Authentication)) - {ScramSha256, ScramSha512}
        subclasses = set(get_args(RESTAuthentication.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"

    def test_kafka_authentication_union(self) -> None:
        """Query and header authentication are valid for REST, and are excluded from Kafka authentication."""
        expected_subclasses = set(get_concrete_subclasses(Authentication)) - {QueryCredentials, HeaderCredentials}
        subclasses = set(get_args(KafkaAuthentication.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"
