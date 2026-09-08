"""Skills API for managing Atlas AI project skills.

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/Skills

Note: This is a beta API and may change in future releases.
"""

from collections.abc import Iterable, Sequence
from urllib.parse import urlencode

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.skill import SkillRequest, SkillResponse


class SkillsAPI(CDFResourceAPI[SkillResponse]):
    """API for managing Atlas AI project skills."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/ai/skills", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/ai/skills/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/ai/skills/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/ai/skills", item_limit=1000),
            },
            api_version="beta",
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[SkillResponse]:
        return PagedResponse[SkillResponse].model_validate_json(response.body)

    def upload(self, items: Sequence[SkillRequest], overwrite: bool = True) -> list[SkillResponse]:
        """Upload SKILL.md content using the dedicated upload endpoint."""
        for item in items:
            query_string = urlencode({"externalId": item.external_id, "overwrite": str(overwrite).lower()})
            result = self._http_client.request_multipart_retries(
                url=self._make_url(f"/ai/skills/upload?{query_string}"),
                files={"file": ("SKILL.md", item.content.encode("utf-8"), "text/markdown")},
                form_fields={},
                api_version=self._api_version,
            )
            if isinstance(result, FailedResponse):
                raise ToolkitAPIError(message=result.body, code=result.status_code)
        return self.retrieve([item.as_id() for item in items], ignore_unknown_ids=False)

    def create(self, items: Sequence[SkillRequest], overwrite: bool = True) -> list[SkillResponse]:
        """Create or update skills in CDF."""
        return self.upload(items, overwrite=overwrite)

    def update(self, items: Sequence[SkillRequest], overwrite: bool = True) -> list[SkillResponse]:
        """Update skills in CDF (implemented as upsert)."""
        return self.upload(items, overwrite=overwrite)

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[SkillResponse]:
        """Retrieve skills from CDF by external ID."""
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete skills from CDF."""
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(self, limit: int | None = None) -> Iterable[list[SkillResponse]]:
        """Iterate over skill summaries in CDF."""
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[SkillResponse]:
        """List skill summaries in CDF."""
        return self._list(limit=limit)
