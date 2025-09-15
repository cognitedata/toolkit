from collections.abc import Sequence

import pytest
import responses
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.extended_filemetadata import (
    ExtendedFileMetadataList,
)
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId


class TestExtendedFileMetadataAPI:
    @pytest.mark.parametrize(
        "instance_id, id, external_id, expected_list",
        [
            pytest.param(("my_space", "myExternalId"), None, "my_file", False, id="External ID with tuple"),
            pytest.param(("my_space", "myExternalId"), 123, None, False, id="ID with tuple"),
            pytest.param(NodeId("my_space", "myExternalId"), None, "my_file", False, id="External ID with NodeId"),
            pytest.param(NodeId("my_space", "myExternalId"), 123, None, False, id="ID with NodeId"),
            pytest.param(
                [PendingInstanceId(NodeId("my_space", "myExternalId"), 123)], None, None, True, id="List of pending IDs"
            ),
        ],
    )
    def test_set_pending_instance_ids_valid_inputs(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[PendingInstanceId],
        id: int | None,
        external_id: str | None,
        expected_list: bool,
        toolkit_config: ToolkitClientConfig,
    ) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/files/set-pending-instance-ids"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "does-not-matter", "id": 123, "createdTime": 0, "lastUpdatedTime": 0}]},
            )
            result = client.files.set_pending_ids(instance_id=instance_id, id=id, external_id=external_id)

        is_list = isinstance(result, ExtendedFileMetadataList)
        assert is_list == expected_list, f"Expected result to be a list: {expected_list}, got {is_list}"

    def test_set_instance_ids_invalid(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        with pytest.raises(TypeError) as excinfo:
            client.files.set_pending_ids(("my_space", "MyExternalId", 1), id=123)

        assert (
            str(excinfo.value)
            == "instance_id must be a NodeId, a tuple of (str, str), or a sequence of PendingIdentifier objects."
        )

    @pytest.mark.parametrize(
        "id, external_id, expected_list",
        [
            pytest.param(None, "my_file", False, id="External ID only"),
            pytest.param(123, None, False, id="ID only"),
            pytest.param([123, 456], None, True, id="List of IDs"),
            pytest.param(None, ["my_file", "my_other_file"], True, id="List of External IDs"),
            pytest.param([123, 456], ["my_file", "my_other_file"], True, id="List of IDs and External IDs"),
        ],
    )
    def test_unlink_instance_ids_valid(
        self,
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None,
        expected_list: bool,
        toolkit_config: ToolkitClientConfig,
    ) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/files/unlink-instance-ids"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "does-not-matter", "id": 123, "createdTime": 0, "lastUpdatedTime": 0}]},
            )
            result = client.files.unlink_instance_ids(id=id, external_id=external_id)

        is_list = isinstance(result, ExtendedFileMetadataList)
        assert is_list == expected_list, f"Expected result to be a list: {expected_list}, got {is_list}"

    def test_unlink_instance_ids_no_identifiers_raises(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        with pytest.raises(ValueError, match=r"At least one of id or external_id must be provided."):
            client.files.unlink_instance_ids(id=None, external_id=None)

    def test_unlink_instance_ids_invalid(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        with pytest.raises(
            ValueError, match=r"Cannot specify both id and external_id as single values. Use one or the other."
        ):
            client.files.unlink_instance_ids(id=123, external_id="123")
