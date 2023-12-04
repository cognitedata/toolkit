from pathlib import Path
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

from cognite_toolkit.cdf_tk.utils import CaseWarning, CDFToolConfig, load_yaml_inject_variables, validate_raw

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "load_data"


def mocked_init(self, client_name: str):
    self._client_name = client_name
    self._client = CogniteClientMock()
    self._data_set_id_by_external_id = {}


def test_init():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig(client_name="cdf-project-templates")
        assert isinstance(instance._client, CogniteClientMock)


def test_dataset_missing_acl():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        with pytest.raises(CogniteAuthError):
            instance = CDFToolConfig(client_name="cdf-project-templates")
            instance.verify_dataset("test")


def test_dataset_create():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig(client_name="cdf-project-templates")
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

    warnings = validate_raw(yaml.safe_load(raw_file.read_text()), TimeSeries, raw_file)

    assert len(warnings) == 2
    assert sorted(warnings) == sorted(
        [
            CaseWarning(raw_file, "wrong_case", "externalId", "is_string", "isString"),
            CaseWarning(raw_file, "wrong_case", "externalId", "is_step", "isStep"),
        ]
    )


def test_validate_raw_nested() -> None:
    raw_file = DATA_FOLDER / "datamodels" / "snake_cased_view_property.yaml"
    warnings = validate_raw(yaml.safe_load(raw_file.read_text()), ViewApply, raw_file)

    assert len(warnings) == 1
    assert warnings == [
        CaseWarning(raw_file, "WorkItem", "externalId", "container_property_identifier", None),
    ]
