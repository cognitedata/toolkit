from cognite.client.data_classes import SequenceWrite
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import SequenceCRUD


class TestSequenceLoader:
    def test_load_timeseries_ref_not_yet_deployed(self) -> None:
        actual_is_dry_run: list[bool] = []

        def mock_id_lookup(
            external_id: str | SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
        ) -> int | list[int]:
            actual_is_dry_run.append(is_dry_run)
            return -1 if isinstance(external_id, str) else [-1] * len(external_id)

        with monkeypatch_toolkit_client() as client:
            client.lookup.data_sets.id.side_effect = mock_id_lookup
            client.lookup.assets.id.side_effect = mock_id_lookup
            client.lookup.security_categories.id.side_effect = mock_id_lookup
            loader = SequenceCRUD.create_loader(client)

        resource = {
            "externalId": "mySequence",
            "columns": [
                {"externalId": "myColumn1"},
                {"externalId": "myColumn2"},
            ],
            "assetExternalId": "my_asset",
            "dataSetExternalId": "my_dataset",
        }

        seq = loader.load_resource(resource.copy(), is_dry_run=True)
        _ = loader.load_resource(resource.copy(), is_dry_run=False)

        assert isinstance(seq, SequenceWrite)
        assert seq.asset_id == -1
        assert seq.data_set_id == -1
        assert actual_is_dry_run == [True] * 2 + [False] * 2
