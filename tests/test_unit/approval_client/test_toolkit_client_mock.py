from tests.test_unit.approval_client.client import LookUpAPIMock


class TestLookUpAPI:
    def test_lookup_dataset(self) -> None:
        external_id = "SomeDataSet"
        lookup = LookUpAPIMock()
        data_set_id = lookup.id(external_id)

        recreated = lookup.external_id(data_set_id)

        assert external_id == recreated
