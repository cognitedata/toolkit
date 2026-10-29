"""Domains API for managing CDF domains."""

import builtins
from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.domain import DomainRequest, DomainResponse


class DomainsAPI(CDFResourceAPI[DomainResponse]):
    """API for managing CDF domains."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/datadomains", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/datadomains/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/datadomains/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/datadomains/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/datadomains", item_limit=100),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DomainResponse]:
        return PagedResponse[DomainResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DomainRequest]) -> builtins.list[DomainResponse]:
        """Create domains in CDF.

        Args:
            items: List of DomainRequest objects to create.
        Returns:
            List of created DomainResponse objects.

        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, external_ids: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[DomainResponse]:
        """Retrieve domains by external ID.

        Args:
            external_ids: List of ExternalId objects of the domains to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs. If False, an error will be raised
                if any of the provided IDs do not exist.
        Returns:
            List of retrieved DomainResponse objects.

        """
        if not external_ids:
            return []
        if ignore_unknown_ids:
            return self._request_item_split_retries(external_ids, method="retrieve")
        return self._request_item_response(external_ids, method="retrieve")

    def update(
        self, items: Sequence[DomainRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[DomainResponse]:
        return self._update(items, mode=mode)

    def delete(self, external_ids: Sequence[ExternalId]) -> None:
        self._request_no_response(external_ids, "delete")

    def paginate(self, limit: int = 10, cursor: str | None = None) -> PagedResponse[DomainResponse]:
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[builtins.list[DomainResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> builtins.list[DomainResponse]:
        return self._list(limit=limit)
