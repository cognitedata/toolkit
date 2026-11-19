"""Resource classes for the read-only extractor catalog.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Extractors
"""

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ExtractorReleaseId


class ExtractorLink(BaseModelObject):
    name: str
    type: str
    url: str


class ExtractorResponse(BaseModelObject):
    """An extractor in the catalog.

    The type is one of ``global``, ``community``, ``unreleased``, ``hosted``, or ``zero_copy``.
    """

    external_id: str
    name: str
    description: str
    type: str
    documentation: str | None = None
    image_url: str | None = None
    latest_version: str | None = None
    links: list[ExtractorLink] | None = None
    tags: list[str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExtractorArtifact(BaseModelObject):
    """A downloadable file of a release. The platform is one of ``windows``, ``linux``, ``macos``, ``docs``, or ``all``."""

    name: str
    link: str
    platform: str
    display_name: str | None = None


class ExtractorChangelog(BaseModelObject):
    """A release changelog in the keep-a-changelog format."""

    added: list[str] | None = None
    changed: list[str] | None = None
    deprecated: list[str] | None = None
    fixed: list[str] | None = None
    removed: list[str] | None = None
    security: list[str] | None = None


class ExtractorReleaseResponse(BaseModelObject):
    """A release of an extractor. The external ID is the external ID of the extractor."""

    external_id: str
    version: str
    artifacts: list[ExtractorArtifact]
    changelog: ExtractorChangelog | None = None
    created_time: int | None = None
    description: str | None = None

    def as_id(self) -> ExtractorReleaseId:
        return ExtractorReleaseId(external_id=self.external_id, version=self.version)


class ExtractorSourceSystemResponse(BaseModelObject):
    """A source system that extractors can read from."""

    external_id: str
    name: str
    description: str
    type: str
    documentation: str | None = None
    image_url: str | None = None
    tags: list[str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExtractorSolutionResponse(BaseModelObject):
    """A connection between an extractor and a source system it can read from."""

    external_id: str
    name: str
    source_system_external_id: str
    extractor_external_id: str | None = None
    documentation: str | None = None
    type: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
