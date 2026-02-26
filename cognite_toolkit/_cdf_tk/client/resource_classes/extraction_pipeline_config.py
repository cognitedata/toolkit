from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExtractionPipelineConfigId


class ExtractionPipelineConfig(BaseModelObject):
    """Base class for extraction pipeline configuration revisions."""

    external_id: str
    config: str | None = None
    description: str | None = None


class ExtractionPipelineConfigRequest(ExtractionPipelineConfig, RequestResource):
    """Request body for creating a new extraction pipeline configuration revision.

    Attributes:
        external_id: External ID of the extraction pipeline this configuration revision belongs to.
        config: Configuration content.
        description: A description of this configuration revision.
    """

    revision: int | None = Field(None, exclude=True)

    def as_id(self) -> ExtractionPipelineConfigId:
        return ExtractionPipelineConfigId(external_id=self.external_id, revision=self.revision)


class ExtractionPipelineConfigResponse(ExtractionPipelineConfig, ResponseResource[ExtractionPipelineConfigRequest]):
    """Response for an extraction pipeline configuration revision.

    Attributes:
        external_id: External ID of the extraction pipeline.
        config: Configuration content.
        revision: The revision number.
        created_time: Timestamp when this revision was created, in milliseconds since epoch.
        description: A description of this configuration revision.
    """

    revision: int
    created_time: int

    @classmethod
    def request_cls(cls) -> type[ExtractionPipelineConfigRequest]:
        return ExtractionPipelineConfigRequest
