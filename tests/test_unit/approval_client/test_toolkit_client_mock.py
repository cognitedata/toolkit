from tests.test_unit.approval_client import ApprovalToolkitClient


class TestLookUpAPI:
    def test_lookup_dataset(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        external_id = "SomeDataSet"
        client = toolkit_client_approval.mock_client
        data_set_id = client.lookup.data_sets.id(external_id)

        recreated = client.lookup.data_sets.external_id(data_set_id)

        assert external_id == recreated
