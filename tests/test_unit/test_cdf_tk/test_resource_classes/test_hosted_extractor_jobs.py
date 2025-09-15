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

    # Missing required fields
    yield pytest.param(
        {
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite"},
        },
        {
            "Missing required field: 'externalId'",
        },
        id="Missing externalId",
    )

    yield pytest.param(
        {
            "externalId": "myJob",
            "sourceId": "mySource",
            "format": {"type": "cognite"},
        },
        {
            "Missing required field: 'destinationId'",
        },
        id="Missing destinationId",
    )

    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "format": {"type": "cognite"},
        },
        {
            "Missing required field: 'sourceId'",
        },
        id="Missing sourceId",
    )

    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
        },
        {
            "Missing required field: 'format'",
        },
        id="Missing format",
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

    # Format validation - invalid encoding
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
        id="Invalid encoding",
    )

    # Format validation - invalid compression
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
        id="Invalid compression",
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

    # DataModelFormat validation - prefix config
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
        id="DataModelFormat prefix too long",
    )

    # DataModelFormat validation - too many data models
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
        id="DataModelFormat too many data models",
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
        id="DataModelFormat missing space",
    )

    # Field length validation - externalId too long
    yield pytest.param(
        {
            "externalId": "a" * 256,
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "cognite"},
        },
        {
            "In field externalId string should have at most 255 characters",
        },
        id="externalId too long",
    )

    # Field length validation - destinationId too long
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "a" * 256,
            "sourceId": "mySource",
            "format": {"type": "cognite"},
        },
        {
            "In field destinationId string should have at most 255 characters",
        },
        id="destinationId too long",
    )

    # Field length validation - sourceId too long
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "a" * 256,
            "format": {"type": "cognite"},
        },
        {
            "In field sourceId string should have at most 255 characters",
        },
        id="sourceId too long",
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

    # RockwellFormat validation tests - additional JobFormat types
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "rockwell", "encoding": "invalid_encoding"},
        },
        {
            "In format.encoding input should be 'utf8', 'utf16', 'utf16le' or 'latin1'. Got 'invalid_encoding'.",
        },
        id="RockwellFormat invalid encoding",
    )

    # ValueFormat validation tests
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "value", "compression": "invalid_compression"},
        },
        {
            "In format.compression input should be 'gzip'. Got 'invalid_compression'.",
        },
        id="ValueFormat invalid compression",
    )

    # More comprehensive DataModelFormat tests
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "rockwell", "prefix": {"fromTopic": True, "prefix": "a" * 256}},
        },
        {
            "In format.prefix.prefix string should have at most 255 characters",
        },
        id="RockwellFormat prefix too long",
    )

    # ValueFormat with dataModels validation
    yield pytest.param(
        {
            "externalId": "myJob",
            "destinationId": "myDest",
            "sourceId": "mySource",
            "format": {"type": "value", "dataModels": [{"space": f"space{i}"} for i in range(11)]},
        },
        {
            "In format.dataModels list should have at most 10 items after validation, not 11",
        },
        id="ValueFormat too many data models",
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
