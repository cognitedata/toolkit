from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.load import DataSetsLoader


class TestDataSetsLoader:
    def test_existing_unchanged(self, cognite_client: CogniteClient):
        data_sets = cognite_client.data_sets.list(limit=1, external_id_prefix="")
        loader = DataSetsLoader(client=cognite_client)

        created, changed, unchanged = loader.to_create_changed_unchanged_triple(data_sets.as_write())

        assert len(unchanged) == len(data_sets)
        assert len(created) == 0
        assert len(changed) == 0
