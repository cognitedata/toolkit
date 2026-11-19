from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestItem

ExtractionPipelineRunStatus = Literal["success", "failure", "seen"]


class ExtractionPipelineRunRequest(RequestItem):
    """Report the status of an extraction pipeline, for example from an extractor after each run.

    "seen" reports that the extractor is alive without a completed run.
    """

    external_id: str
    status: ExtractionPipelineRunStatus
    message: str | None = None
    created_time: int | None = None

    def __str__(self) -> str:
        return f"externalId='{self.external_id}', status='{self.status}'"


class ExtractionPipelineRunResponse(BaseModelObject):
    id: int | None = None
    status: str
    message: str | None = None
    created_time: int | None = None
    # Only returned when the run is created.
    external_id: str | None = None
