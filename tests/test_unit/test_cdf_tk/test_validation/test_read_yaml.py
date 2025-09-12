from collections.abc import Iterable
from pathlib import Path

import pytest
import yaml
from cognite.client.data_classes import TimeSeries

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, SpaceCRUD, TimeSeriesCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.tk_warnings import (
    CaseTypoWarning,
    DataSetMissingWarning,
    MissingRequiredParameterWarning,
    ToolkitWarning,
    UnusedParameterWarning,
)
from cognite_toolkit._cdf_tk.validation import validate_data_set_is_set, validate_resource_yaml
from tests.data import LOAD_DATA

DUMMY_FILE = Path("dummy.yaml")


def test_validate_raw() -> None:
    raw_file = LOAD_DATA / "timeseries" / "wrong_case.yaml"

    warnings = validate_resource_yaml(
        yaml.safe_load(raw_file.read_text()), TimeSeriesCRUD.get_write_cls_parameter_spec(), raw_file
    )

    assert len(warnings) == 2
    assert sorted(warnings) == sorted(
        [
            CaseTypoWarning(raw_file, 1, ("is_string",), "is_string", "isString"),
            CaseTypoWarning(raw_file, 1, ("is_step",), "is_step", "isStep"),
        ]
    )


def test_validate_raw_nested() -> None:
    raw_file = LOAD_DATA / "datamodels" / "snake_cased_view_property.yaml"
    warnings = validate_resource_yaml(
        yaml.safe_load(raw_file.read_text()), ViewCRUD.get_write_cls_parameter_spec(), raw_file
    )

    assert len(warnings) == 1
    assert warnings == [
        CaseTypoWarning(
            raw_file,
            None,
            ("properties", "criticality", "container_property_identifier"),
            "container_property_identifier",
            "containerPropertyIdentifier",
        )
    ]


def test_validate_data_set_is_set():
    warnings = validate_data_set_is_set(
        {"externalId": "myTimeSeries", "name": "My Time Series"}, TimeSeries, Path("timeseries.yaml")
    )

    assert sorted(warnings) == sorted(
        [DataSetMissingWarning(Path("timeseries.yaml"), "myTimeSeries", "externalId", "TimeSeries")]
    )


def validate_yaml_config_test_cases() -> Iterable:
    content = """space: my_space
"""
    yield pytest.param(content, SpaceCRUD.get_write_cls_parameter_spec(), [], id="Valid space")

    content = """space: my_space
nme: My space
"""
    yield pytest.param(
        content,
        SpaceCRUD.get_write_cls_parameter_spec(),
        [
            UnusedParameterWarning(
                DUMMY_FILE,
                None,
                ("nme",),
                "nme",
            )
        ],
        id="Unused parameter",
    )
    content = """externalId: Pump
name: Pump
usedFor: node
properties:
  DesignPointFlowGPM:
    autoIncrement: false
    default_value: astring
    nullable: true
    type:
      list: false
      type: float64"""
    yield pytest.param(
        content,
        ContainerCRUD.get_write_cls_parameter_spec(),
        [
            MissingRequiredParameterWarning(DUMMY_FILE, None, ("space",), "space"),
            CaseTypoWarning(
                DUMMY_FILE, None, ("properties", "DesignPointFlowGPM", "default_value"), "default_value", "defaultValue"
            ),
        ],
        id="Missing required and misspelling",
    )
    content = """
- space: sp_my_space
  name: First space
- space: sp_my_space_2
  nme: Second space
- space: sp_my_space_3
  nme: Third space"""
    yield pytest.param(
        content,
        SpaceCRUD.get_write_cls_parameter_spec(),
        [
            UnusedParameterWarning(DUMMY_FILE, 2, ("nme",), "nme"),
            UnusedParameterWarning(DUMMY_FILE, 3, ("nme",), "nme"),
        ],
        id="Unused parameter in list",
    )


class TestValidateYAML:
    @pytest.mark.parametrize("content, spec, expected_warnings", list(validate_yaml_config_test_cases()))
    def test_validate_yaml_config(self, content: str, spec: ParameterSpecSet, expected_warnings: list[ToolkitWarning]):
        data = yaml.safe_load(content)
        warnings = validate_resource_yaml(data, spec, DUMMY_FILE)
        assert sorted(warnings) == sorted(expected_warnings)
