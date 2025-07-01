import abc

from typing import Literal
from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRunWrite


class IPipelineService(abc.ABC):
    """
    Interface for creating and updating extraction pipeline logs.
    """

    @abc.abstractmethod
    def update_extraction_pipeline(self, msg: str) -> None:
        pass

    @abc.abstractmethod
    def upload_extraction_pipeline(
        self,
        status: Literal["success", "failure", "seen"],
    ) -> None:
        pass


class GeneralPipelineService(IPipelineService):
    """
    Implementation of the pipeline interface
    """

    def __init__(self, pipeline_ext_id: str, client: CogniteClient):
        self.client: CogniteClient = client
        self.ep_write: ExtractionPipelineRunWrite = ExtractionPipelineRunWrite(
            extpipe_external_id=pipeline_ext_id,
            status="seen",
        )

    def update_extraction_pipeline(self, msg: str) -> None:
        """
        Update the message log for the extraction pipeline
        """
        if not self.ep_write.message:
            self.ep_write.message = msg
        else:
            self.ep_write.message = f"{self.ep_write.message}\n{msg}"

    def upload_extraction_pipeline(
        self,
        status: Literal["success", "failure", "seen"],
    ) -> None:
        """
        Upload the extraction pipeline run so that status and message logs are captured
        """
        self.ep_write.status = status
        self.client.extraction_pipelines.runs.create(self.ep_write)
