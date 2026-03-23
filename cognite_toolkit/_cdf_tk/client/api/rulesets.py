"""Rule Sets API for managing CDF rule sets."""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.ruleset import RuleSetRequest, RuleSetResponse

from .ruleset_versions import RuleSetVersionsAPI


class RuleSetsAPI(CDFResourceAPI[RuleSetResponse]):
    """API for managing CDF rule sets."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/rulesets", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/rulesets/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/rulesets/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/rulesets", item_limit=100),
            },
            api_version="alpha",
        )
        self.versions = RuleSetVersionsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RuleSetResponse]:
        return PagedResponse[RuleSetResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RuleSetRequest]) -> list[RuleSetResponse]:
        return self._request_item_response(items, "create")

    def retrieve(self, external_ids: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[RuleSetResponse]:
        if not external_ids:
            return []
        return self._request_item_response(
            external_ids, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, external_ids: Sequence[ExternalId]) -> None:
        self._request_no_response(external_ids, "delete")

    def paginate(self, limit: int = 10, cursor: str | None = None) -> PagedResponse[RuleSetResponse]:
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[RuleSetResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[RuleSetResponse]:
        return self._list(limit=limit)
