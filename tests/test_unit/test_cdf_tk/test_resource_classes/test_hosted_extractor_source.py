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
        {"In authentication missing required field: 'password'"},
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
                "scope": "scope",
                # missing client_secret
            },
        },
        {
            "In authentication missing required field: 'clientId'",
            "In authentication missing required field: 'clientSecret'",
            "In authentication missing required field: 'tokenUrl'",
            "In authentication unused field: 'client_id'",
            "In authentication unused field: 'token_url'",
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
        {"In authentication missing required field: 'value'"},
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
        {"In authentication missing required field: 'value'"},
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
            "In field authentication invalid authentication type 'scramSha256' for REST source. Expected one of basic, clientCredentials, header or query"
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
        {"In authentication missing required field: 'password'"},
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
        {"In authentication missing required field: 'password'"},
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
                "scope": "scope",
                # missing client_secret
            },
        },
        {"In authentication missing required field: 'clientSecret'"},
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
            "In field authentication invalid authentication type 'query' for Kafka source. Expected one of basic, clientCredentials, scramSha256 or scramSha512"
        },
        id="KafkaSource QueryCredentials invalid type for Kafka",
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
