from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.hosted_extractor_job import HostedExtractorJobYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_hosted_extractor_job_test_cases() -> Iterable:
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "invalid", "some_field": "some_value"},
            "config": {"topicFilter": "some_filter"},
        },
        {
            "In field format invalid type 'invalid'. Expected one of cognite, custom, rockwell or value",
        },
        id="Invalid type",
    )

    yield pytest.param(
        {
            "externalId": "myJob",
        },
        {
            "Missing required field: 'destinationId'",
            "Missing required field: 'sourceId'",
            "Missing required field: 'format'",
        },
        id="Missing required fields",
    )

    # Format validation - missing type
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"encoding": "utf8"},
        },
        {
            "In field format invalid input format missing 'type' key",
        },
        id="Format missing type",
    )

    # Format validation - invalid encoding for all JobFormat types
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite", "encoding": "invalid_encoding"},
        },
        {
            "In format.encoding input should be 'utf8', 'utf16', 'utf16le' or 'latin1'. Got 'invalid_encoding'.",
        },
        id="CogniteFormat invalid encoding",
    )

    # Format validation - invalid compression for all JobFormat types
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite", "compression": "invalid_compression"},
        },
        {
            "In format.compression input should be 'gzip'. Got 'invalid_compression'.",
        },
        id="CogniteFormat invalid compression",
    )

    # CustomFormat validation - missing mapping_id
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "custom"},
        },
        {
            "In format missing required field: 'mappingId'",
        },
        id="CustomFormat missing mappingId",
    )

    # CustomFormat validation - too long mapping_id
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "custom", "mappingId": "a" * 256},
        },
        {
            "In format.mappingId string should have at most 255 characters",
        },
        id="CustomFormat mappingId too long",
    )

    # DataModelFormat validation - prefix config for all DataModelFormat types
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite", "prefix": {"prefix": "a" * 256}},
        },
        {
            "In format.prefix.prefix string should have at most 255 characters",
        },
        id="CogniteFormat prefix too long",
    )

    # DataModelFormat validation - too many data models for all DataModelFormat types
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite", "dataModels": [{"space": f"space{i}"} for i in range(11)]},
        },
        {
            "In format.dataModels list should have at most 10 items after validation, not 11",
        },
        id="CogniteFormat too many data models",
    )

    # DataModelFormat validation - missing space in dataModels
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite", "dataModels": [{}]},
        },
        {
            "In format.dataModels[1] missing required field: 'space'",
        },
        id="CogniteFormat missing space in dataModels",
    )

    # Field length validation - externalId too long
    yield pytest.param(
        {
            "externalId": "a" * 256,
            "destinationId": "a" * 256,
            "sourceId": "a" * 256,
            "format": {"type": "cognite"},
        },
        {
            "In field externalId string should have at most 255 characters",
            "In field destinationId string should have at most 255 characters",
            "In field sourceId string should have at most 255 characters",
        },
        id="externalId too long",
    )

    # Format validation - non-dict format
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": "invalid_format",
        },
        {
            "In field format invalid input for format '<class 'str'>' expected dict",
        },
        id="Format non-dict type",
    )

    # Format validation - null format
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": None,
        },
        {
            "In field format invalid input for format '<class 'NoneType'>' expected dict",
        },
        id="Format null type",
    )

    # Rest Config validation - invalid IncrementalLoad type
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite"},
            "config": {
                "interval": "1h",
                "path": "/some/path",
                "method": "get",
                "incrementalLoad": {"type": "nextUrl", "value": "some_value"},
                "pagination": {"type": "body", "value": "some_value"},
            },
        },
        {
            "In config.KafkaConfig missing required field: 'topic'",
            "In config.KafkaConfig unused field: 'incrementalLoad'",
            "In config.KafkaConfig unused field: 'interval'",
            "In config.KafkaConfig unused field: 'method'",
            "In config.KafkaConfig unused field: 'pagination'",
            "In config.KafkaConfig unused field: 'path'",
            "In config.MQTTConfig missing required field: 'topicFilter'",
            "In config.MQTTConfig unused field: 'incrementalLoad'",
            "In config.MQTTConfig unused field: 'interval'",
            "In config.MQTTConfig unused field: 'method'",
            "In config.MQTTConfig unused field: 'pagination'",
            "In config.MQTTConfig unused field: 'path'",
            "In config.RestConfig.incrementalLoad invalid type 'nextUrl'. Expected one of "
            "body, headerValue and queryParameter",
        },
        id="Invalid IncrementalLoad and Pagination type",
    )


class TestHostedExtractorJobYAML:
    @pytest.mark.parametrize("data", list(find_resources("Job", resource_dir="hosted_extractors")))
    def test_load_valid_hosted_extractor_job(self, data: dict[str, object]) -> None:
        loaded = HostedExtractorJobYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_hosted_extractor_job_test_cases()))
    def test_invalid_hosted_extractor_job_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, HostedExtractorJobYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
