import pytest
from cognite.client import CogniteClient

from cognite_toolkit.cdf_tk.load import DataSetsLoader, ExtractionPipelineConfigLoader


class TestDataSetsLoader:
    def test_existing_unchanged(self, cognite_client: CogniteClient):
        data_sets = cognite_client.data_sets.list(limit=1, external_id_prefix="")
        loader = DataSetsLoader(client=cognite_client)

        created, changed, unchanged = loader.to_create_changed_unchanged_triple(data_sets.as_write())

        assert len(unchanged) == len(data_sets)
        assert len(created) == 0
        assert len(changed) == 0


class TestExtractionPipelineLoader:
    def test_existing_unchanged(self, cognite_client: CogniteClient):

        extraction_pipeline = cognite_client.extraction_pipelines.list(limit=1)
        if len(extraction_pipeline) == 0:
            pytest.skip("No extraction pipelines found")
        extraction_pipeline_config = cognite_client.extraction_pipelines.config.retrieve(
            extraction_pipeline[0].external_id
        )
        loader = ExtractionPipelineConfigLoader(client=CogniteClient)

        created, changed, unchanged = loader.to_create_changed_unchanged_triple(extraction_pipeline_config.as_write())

        assert len(unchanged) == 0
        assert len(created) == 1
        assert len(changed) == 0
