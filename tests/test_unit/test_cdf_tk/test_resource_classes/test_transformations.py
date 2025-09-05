from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import TransformationYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def transformation_destination_cases() -> Iterable:
    transformation_destination = [
        {
            "DataModelSource": {
                "externalId": "tr_first_transformation",
                "name": "example:first:transformation",
                "ignoreNullFields": True,
                "destination": {
                    "type": "instances",
                    "dataModel": {
                        "externalId": "my_data_model",
                        "version": "1",
                        "space": "my_space",
                        "destinationType": "my_view",
                    },
                    "instanceSpace": "my_instance_space",
                },
            }
        },
        {
            "DataModelSource": {
                "externalId": "tr_first_transformation",
                "name": "example:first:transformation",
                "ignoreNullFields": True,
                "destination": {
                    "type": "instances",
                    "dataModel": {
                        "externalId": "my_data_model",
                        "version": "1",
                        "space": "my_space",
                        "destinationType": "my_view",
                    },
                },
            }
        },
        {
            "ViewDataSource": {
                "externalId": "tr_first_transformation",
                "name": "example:first:transformation",
                "ignoreNullFields": True,
                "destination": {
                    "type": "nodes",
                    "view": {"externalId": "my_view", "version": "1", "space": "my_space"},
                    "edgeType": {"externalId": "my_edge_type", "space": "my_space"},
                },
            }
        },
        {
            "RawDataSource": {
                "externalId": "tr_first_transformation",
                "name": "example:first:transformation",
                "ignoreNullFields": True,
                "destination": {"type": "raw", "database": "my_database", "table": "my_table"},
            }
        },
        {
            "SequenceRowDataSource": {
                "externalId": "tr_first_transformation",
                "name": "example:first:transformation",
                "ignoreNullFields": True,
                "destination": {"type": "sequence_rows", "externalId": "my_sequence"},
            }
        },
    ]
    yield from (pytest.param(next(iter(des.values())), id=next(iter(des.keys()))) for des in transformation_destination)


def invalid_transformation_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "my_transformation", "ignoreNullFields": True},
        {"Missing required field: 'externalId'"},
        id="Missing externalId",
    )
    yield pytest.param(
        {
            "conflictMode": "upsert",
            "dataSetExternalId": "ops:001:monitor",
            "destination": {"type": "raw", "database": "ops:001:monitor:rawtable", "table": "rawtable"},
            "authentication.write": {
                "cdfProjectName": "my_project",
                "clientId": "my_client_id",
                "clientSecret": "my_client_secret",
                "scopes": ["USER_MANAGEMENT", "DATASETS_WRITE"],
                "tokenUri": "https://api.cognitedata.com/api/v1/oauth/token",
            },
            "externalId": "cdf_ops:raw:upsert:001:monitor",
            "ignoreNullFields": True,
            "isPublic": True,
            "name": "cdf_ops:raw:upsert:001:monitor",
            "authentication.read": {
                "cdfProjectName": "my_project",
                "clientId": "my_client_id",
                "clientSecret": "my_client_secret",
                "scopes": ["USER_MANAGEMENT", "DATASETS_READ"],
                "tokenUri": "https://api.cognitedata.com/api/v1/oauth/token",
            },
        },
        {"Unused field: 'authentication.read'", "Unused field: 'authentication.write'"},
        id="Invalid authentication - base on real use case",
    )
    yield pytest.param(
        {
            "externalId": "tr_invalid_type",
            "name": "Invalid ignore_null_fields",
            "ignoreNullFields": "yes",
        },
        {"In field ignoreNullFields input should be a valid boolean. Got 'yes' of type str."},
        id="Invalid ignore_null_fields type",
    )
    yield pytest.param(
        {
            "externalId": "tr_too_many_tags",
            "name": "Too many tags",
            "ignoreNullFields": True,
            "tags": ["a", "b", "c", "d", "e", "f"],
        },
        {"In field tags list should have at most 5 items after validation, not 6"},
        id="Too many tags in transformation",
    )
    yield pytest.param(
        {
            "externalId": "tr_invalid_conflict",
            "name": "Invalid conflict_mode",
            "ignoreNullFields": True,
            "conflictMode": "replace",
        },
        {"In field conflictMode input should be 'abort', 'delete', 'update' or 'upsert'. Got 'replace'."},
        id="Invalid conflict_mode value",
    )
    yield pytest.param(
        {
            "externalId": "tr_invalid_auth",
            "name": "Invalid authentication",
            "ignoreNullFields": True,
            "authentication": {"invalid_key": "value"},
        },
        {
            "In authentication missing required field: 'clientId'",
            "In authentication missing required field: 'clientSecret'",
            "In authentication unused field: 'invalid_key'",
        },
        id="Invalid authentication type",
    )


class TestTransformationYAML:
    @pytest.mark.parametrize("data", list(find_resources("transformation")))
    def test_load_valid_transformation_file(self, data: dict[str, object]) -> None:
        loaded = TransformationYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        if "authentication" in dumped:
            # Secret is not dumped as per design, so we add it back for comparison
            dumped["authentication"]["clientSecret"] = data["authentication"]["clientSecret"]
        assert dumped == data

    @pytest.mark.parametrize("data", transformation_destination_cases())
    def test_load_valid_transformation_destination_parameters(self, data: dict[str, object]) -> None:
        loaded = TransformationYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_transformation_test_cases()))
    def test_invalid_transformation_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        warning_list = validate_resource_yaml_pydantic(data, TransformationYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
