from pathlib import Path

import pytest
import yaml
from cognite.client.data_classes import (
    DataSet,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigWrite,
    ExtractionPipelineWrite,
)
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import ExtractionPipelineFinder
from cognite_toolkit._cdf_tk.cruds import (
    ExtractionPipelineConfigCRUD,
    ExtractionPipelineCRUD,
)


@pytest.fixture(scope="session")
def deployed_extraction_pipeline(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> ExtractionPipeline:
    pipeline = ExtractionPipelineWrite(
        external_id="toolkit_test_extraction_pipeline",
        name="Toolkit Test ExtractionPipeline",
        data_set_id=toolkit_dataset.id,
        description="This is used in integration tests of the toolkit.",
        created_by="Cognite Toolkit",
    )
    existing = toolkit_client.extraction_pipelines.retrieve(external_id=pipeline.external_id)
    if existing:
        return existing
    return toolkit_client.extraction_pipelines.create(pipeline)


@pytest.fixture(scope="session")
def deployed_extraction_pipeline_config(
    toolkit_client: ToolkitClient, deployed_extraction_pipeline: ExtractionPipeline
) -> ExtractionPipelineConfig:
    config = ExtractionPipelineConfigWrite(
        external_id=deployed_extraction_pipeline.external_id,
        config=yaml.safe_dump({"some": "config"}),
        description="This is used in integration tests of the toolkit.",
    )
    try:
        return toolkit_client.extraction_pipelines.config.retrieve(external_id=deployed_extraction_pipeline.external_id)
    except CogniteAPIError as e:
        if e.code == 404:
            # If the config does not exist, create it
            return toolkit_client.extraction_pipelines.config.create(config)
        raise e


class TestDumpExtractionPipeline:
    @pytest.mark.usefixtures(
        "deployed_extraction_pipeline_config",
    )
    def test_dump_extraction_pipeline_with_config(
        self,
        deployed_extraction_pipeline: ExtractionPipeline,
        toolkit_client: ToolkitClient,
        tmp_path: Path,
    ) -> None:
        cmd = DumpResourceCommand(silent=True)
        cmd.dump_to_yamls(
            ExtractionPipelineFinder(toolkit_client, (deployed_extraction_pipeline.external_id,)),
            output_dir=tmp_path,
            clean=False,
            verbose=False,
        )

        folder = tmp_path / ExtractionPipelineCRUD.folder_name
        assert folder.exists()
        assert sum(1 for _ in folder.glob(f"*{ExtractionPipelineCRUD.kind}.yaml")) == 1
        assert sum(1 for _ in folder.glob(f"*{ExtractionPipelineConfigCRUD.kind}.yaml")) == 1
