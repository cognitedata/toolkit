from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.resource_classes import TransformationYAML
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


class TestTransformationYAML:
    @pytest.mark.parametrize("data", list(find_resources("transformation")))
    def test_load_valid_transformation_file(self, data: dict[str, object]) -> None:
        loaded = TransformationYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", transformation_destination_cases())
    def test_load_valid_transformation_destination_parameters(self, data: dict[str, object]) -> None:
        loaded = TransformationYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
