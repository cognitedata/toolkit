from unittest.mock import Mock, patch

from cognite.client.testing import CogniteClientMock
from cognite.client._api.iam import TokenAPI, TokenInspection
from cognite.client._api.assets import AssetsAPI

from cognite.client.exceptions import CogniteAuthError
import pytest
from scripts.utils import CDFToolConfig


def mocked_init(self, client_name: str):
    self._client_name = client_name
    self._client = CogniteClientMock()

def test_init():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance =  CDFToolConfig(client_name="cdf-project-templates")
        assert isinstance(instance._client, CogniteClientMock)


def test_dataset_missing_acl():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        with pytest.raises(CogniteAuthError):
            instance =  CDFToolConfig(client_name="cdf-project-templates")
            instance.verify_dataset("test")


def test_dataset_create():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig(client_name="cdf-project-templates")
        instance._client.iam.token.inspect = Mock(
            spec=TokenAPI.inspect, 
            return_value=
                TokenInspection(
                    subject="", 
                    capabilities=[{
                        "datasetsAcl": {
                            "actions": [
                                "READ",
                                "WRITE"
                            ],
                            "scope": {
                                "all": {}
                            }
                        },
                        "projectScope": {
                            "projects": [
                                "cdf-project-templates"
                            ]
                        }
                    }],
                    projects=[{"name": "cdf-project-templates"}]))
        
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


