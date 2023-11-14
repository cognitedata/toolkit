from unittest.mock import Mock, patch

import pytest
import yaml
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.iam import TokenAPI, TokenInspection
from cognite.client.data_classes.capabilities import (
    DataSetsAcl,
    ProjectCapabilitiesList,
    ProjectCapability,
    ProjectsScope,
)
from cognite.client.data_classes.iam import ProjectSpec
from cognite.client.exceptions import CogniteAuthError
from cognite.client.testing import CogniteClientMock

from cognite_toolkit.cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables


def mocked_init(self, client_name: str):
    self._client_name = client_name
    self._client = CogniteClientMock()


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
                capabilities=ProjectCapabilitiesList(
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

        # the dataset does not exist, do not create
        instance._client.data_sets.retrieve = Mock(spec=AssetsAPI.retrieve, return_value=None)
        instance.verify_dataset("test", False)
        assert instance._client.data_sets.create.call_count == 0

        # the dataset does not exist, create
        instance.verify_dataset("test", True)
        assert instance._client.data_sets.create.call_count == 1


def test_load_yaml_inject_variables(tmp_path) -> None:
    my_file = tmp_path / "test.yaml"
    my_file.write_text(yaml.safe_dump({"test": "${TEST}"}))

    loaded = load_yaml_inject_variables(my_file, {"TEST": "my_injected_value"})

    assert loaded["test"] == "my_injected_value"
