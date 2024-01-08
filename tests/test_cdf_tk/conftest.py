from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from cognite.client._api.iam import TokenAPI, TokenInspection
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.capabilities import (
    Capability,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsScope,
)
from cognite.client.data_classes.iam import ProjectSpec
from cognite.client.testing import CogniteClientMock
from requests import Response

from cognite_toolkit.cdf_tk.utils import CDFToolConfig


def mocked_init(self):
    self._client = CogniteClientMock()
    self._data_set_id_by_external_id = {}


@pytest.fixture
def MockCDFToolConfig() -> CDFToolConfig:
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig()
        instance._project = "cdf-project-templates"
        instance.oauth_credentials = MagicMock(spec=OAuthClientCredentials)
        instance.oauth_credentials.authorization_header = Mock(spec=(str, str), return_value=("Bearer", "123"))
        instance._client.config.project = "cdf-project-templates"
        yield instance


def get_capabilities_mock(capabilities: list[Capability]) -> Mock:
    project_capability_list: list(ProjectCapability) = []
    for capability in capabilities:
        project_capability_list.append(
            ProjectCapability(
                capability=capability,
                project_scope=ProjectsScope(["cdf-project-templates"]),
            )
        )
    return Mock(
        spec=TokenAPI.inspect,
        return_value=TokenInspection(
            subject="",
            capabilities=ProjectCapabilityList(project_capability_list, cognite_client=CDFToolConfig()._client),
            projects=[ProjectSpec(url_name="cdf-project-templates", groups=[])],
        ),
    )


def get_post_mock(spec: dict[str, Any], content: str) -> Mock:
    sessionResponse = Response()
    sessionResponse.status_code = 200
    sessionResponse._content = content
    return Mock(
        spec=spec,
        return_value=sessionResponse,
    )
