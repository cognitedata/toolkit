from unittest.mock import MagicMock

import pytest
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import WorkflowTriggerUpsert

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.loaders import WorkflowTriggerLoader


class TestWorkflowTriggerLoader:
    @pytest.mark.skipif(
        not Flags.STRICT_VALIDATION.is_enabled(), reason="This test is only relevant when strict validation is enabled"
    )
    def test_credentials_missing_raise(self) -> None:
        schedule = dict(
            externalId="daily-8am-utc",
            triggerRule=dict(
                triggerType="schedule",
                cronExpression="0 8 * * *",
            ),
            workflowExternalId="wf_example_repeater",
            workflowVersion="v1",
        )
        config = MagicMock(spec=ToolkitClientConfig)
        config.is_strict_validation = True
        config.credentials = OAuthClientCredentials(
            client_id="toolkit-client-id",
            client_secret="toolkit-client-secret",
            token_url="https://cognite.com/token",
            scopes=["USER_IMPERSONATION"],
        )
        with monkeypatch_toolkit_client() as client:
            client.config = config
            loader = WorkflowTriggerLoader.create_loader(client)

        with pytest.raises(ToolkitRequiredValueError):
            loader.load_resource(schedule, is_dry_run=False)
        client.config.is_strict_validation = False
        loaded = loader.load_resource(schedule, is_dry_run=False)
        assert isinstance(loaded, WorkflowTriggerUpsert)
        credentials = loader._authentication_by_id[loader.get_id(loaded)]
        assert credentials.client_id == "toolkit-client-id"
        assert credentials.client_secret == "toolkit-client-secret"
