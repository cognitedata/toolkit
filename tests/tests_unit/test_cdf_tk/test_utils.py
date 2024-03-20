import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml
from cognite.client._api.iam import TokenAPI, TokenInspection
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.capabilities import (
    DataSetsAcl,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsScope,
)
from cognite.client.data_classes.data_modeling import ViewApply
from cognite.client.data_classes.iam import ProjectSpec
from cognite.client.exceptions import CogniteAuthError
from cognite.client.testing import CogniteClientMock

from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    DataSetMissingWarning,
    SnakeCaseWarning,
    TemplateVariableWarning,
    calculate_directory_hash,
    load_yaml_inject_variables,
    validate_case_raw,
    validate_data_set_is_set,
    validate_modules_variables,
)

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "load_data"


def mocked_init(self):
    self._client = CogniteClientMock()
    self._cache = CDFToolConfig._Cache()


def test_init():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig()
        assert isinstance(instance._client, CogniteClientMock)


@pytest.mark.skip("Rewrite to use ApprovalClient")
def test_dataset_missing_acl():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        with pytest.raises(CogniteAuthError):
            instance = CDFToolConfig()
            instance.verify_dataset("test")


def test_dataset_create():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig()
        instance._client.config.project = "cdf-project-templates"
        instance._client.iam.token.inspect = Mock(
            spec=TokenAPI.inspect,
            return_value=TokenInspection(
                subject="",
                capabilities=ProjectCapabilityList(
                    [
                        ProjectCapability(
                            capability=DataSetsAcl(
                                [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write], scope=DataSetsAcl.Scope.All()
                            ),
                            project_scope=ProjectsScope(["cdf-project-templates"]),
                        )
                    ],
                    cognite_client=instance._client,
                ),
                projects=[ProjectSpec(url_name="cdf-project-templates", groups=[])],
            ),
        )

        # the dataset exists
        instance.verify_dataset("test")
        assert instance._client.data_sets.retrieve.call_count == 1


def test_load_yaml_inject_variables(tmp_path) -> None:
    my_file = tmp_path / "test.yaml"
    my_file.write_text(yaml.safe_dump({"test": "${TEST}"}))

    loaded = load_yaml_inject_variables(my_file, {"TEST": "my_injected_value"})

    assert loaded["test"] == "my_injected_value"


def test_validate_raw() -> None:
    raw_file = DATA_FOLDER / "timeseries" / "wrong_case.yaml"

    warnings = validate_case_raw(yaml.safe_load(raw_file.read_text()), TimeSeries, raw_file)

    assert len(warnings) == 2
    assert sorted(warnings) == sorted(
        [
            SnakeCaseWarning(raw_file, "wrong_case", "externalId", "is_string", "isString"),
            SnakeCaseWarning(raw_file, "wrong_case", "externalId", "is_step", "isStep"),
        ]
    )


def test_validate_raw_nested() -> None:
    raw_file = DATA_FOLDER / "datamodels" / "snake_cased_view_property.yaml"
    warnings = validate_case_raw(yaml.safe_load(raw_file.read_text()), ViewApply, raw_file)

    assert len(warnings) == 1
    assert warnings == [
        SnakeCaseWarning(
            raw_file, "WorkItem", "externalId", "container_property_identifier", "containerPropertyIdentifier"
        )
    ]


@pytest.mark.parametrize(
    "config_yaml, expected_warnings",
    [
        pytest.param(
            {"sourceId": "<change_me>"},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "")],
            id="Single warning",
        ),
        pytest.param(
            {"a_module": {"sourceId": "<change_me>"}},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_module")],
            id="Nested warning",
        ),
        pytest.param(
            {"a_super_module": {"a_module": {"sourceId": "<change_me>"}}},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_super_module.a_module")],
            id="Deep nested warning",
        ),
        pytest.param({"a_module": {"sourceId": "123"}}, [], id="No warning"),
    ],
)
def test_validate_config_yaml(config_yaml: dict[str, Any], expected_warnings: list[TemplateVariableWarning]) -> None:
    warnings = validate_modules_variables(config_yaml, Path("config.yaml"))

    assert sorted(warnings) == sorted(expected_warnings)


def test_validate_data_set_is_set():
    warnings = validate_data_set_is_set(
        {"externalId": "myTimeSeries", "name": "My Time Series"}, TimeSeries, Path("timeseries.yaml")
    )

    assert sorted(warnings) == sorted(
        [DataSetMissingWarning(Path("timeseries.yaml"), "myTimeSeries", "externalId", "TimeSeries")]
    )


def test_calculate_hash_on_folder():
    folder = Path(THIS_FOLDER / "calc_hash_data")
    hash1 = calculate_directory_hash(folder)
    hash2 = calculate_directory_hash(folder)

    print(hash1)

    assert (
        hash1 == "e60120ed03ebc1de314222a6a330dce08b7e2d77ec0929cd3c603cfdc08999ad"
    ), f"The hash should not change as long as content in {folder} is not changed."
    assert hash1 == hash2
    tempdir = Path(tempfile.mkdtemp())
    shutil.rmtree(tempdir)
    shutil.copytree(folder, tempdir)
    hash3 = calculate_directory_hash(tempdir)
    shutil.rmtree(tempdir)

    assert hash1 == hash3
