"""Extractors API for the read-only extractor catalog: extractors, releases, solutions, and source systems.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Extractors

The list endpoints do not page and always return the full catalog, so ``limit`` truncates client side.
"""

import builtins
from abc import ABC
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from pydantic import JsonValue, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import T_BaseModelObject
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ExtractorReleaseId
from cognite_toolkit._cdf_tk.client.resource_classes.extractor import (
    ExtractorReleaseResponse,
    ExtractorResponse,
    ExtractorSolutionResponse,
    ExtractorSourceSystemResponse,
)

_ITEM_LIMIT = 100


class _ExtractorCatalogAPI(CDFResourceAPI[T_BaseModelObject], ABC):
    def __init__(self, http_client: HTTPClient, path: str) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path=path, item_limit=_ITEM_LIMIT),
                "retrieve": Endpoint(method="POST", path=f"{path}/byids", item_limit=_ITEM_LIMIT),
            },
        )

    def _list_catalog(
        self, limit: int | None, params: dict[str, Any] | None = None
    ) -> builtins.list[T_BaseModelObject]:
        if limit is not None and limit < 1:
            raise ValueError(f"Limit must be at least 1, got {limit}.")
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            parameters=self._filter_out_none_values(params),
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        items = self._validate_page_response(response).items
        return items if limit is None else items[:limit]


class ExtractorReleasesAPI(_ExtractorCatalogAPI[ExtractorReleaseResponse]):
    """API for releases of the extractors in the catalog."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, "/extractors/releases")

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractorReleaseResponse]:
        return PagedResponse[ExtractorReleaseResponse].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[ExtractorReleaseId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ExtractorReleaseResponse]:
        """Retrieve releases by extractor external ID and version.

        Args:
            items: The releases to retrieve.
            ignore_unknown_ids: Ignore releases that are not found.
        Returns:
            The retrieved releases.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list(
        self, extractor_external_id: str | None = None, limit: int | None = None
    ) -> builtins.list[ExtractorReleaseResponse]:
        """List extractor releases.

        Args:
            extractor_external_id: Only return releases of this extractor.
            limit: Maximum number of releases to return. None returns all.
        Returns:
            The releases.
        """
        return self._list_catalog(limit, params={"externalId": extractor_external_id})


class ExtractorSolutionsAPI(_ExtractorCatalogAPI[ExtractorSolutionResponse]):
    """API for solutions, which connect an extractor to a source system it can read from."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, "/extractors/solutions")

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractorSolutionResponse]:
        return PagedResponse[ExtractorSolutionResponse].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ExtractorSolutionResponse]:
        """Retrieve solutions by external ID.

        Args:
            items: The external IDs of the solutions to retrieve.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved solutions.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list(self, limit: int | None = None) -> builtins.list[ExtractorSolutionResponse]:
        """List solutions.

        Args:
            limit: Maximum number of solutions to return. None returns all.
        Returns:
            The solutions.
        """
        return self._list_catalog(limit)


class ExtractorSourceSystemsAPI(_ExtractorCatalogAPI[ExtractorSourceSystemResponse]):
    """API for the source systems that extractors can read from."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, "/extractors/sources")

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractorSourceSystemResponse]:
        return PagedResponse[ExtractorSourceSystemResponse].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ExtractorSourceSystemResponse]:
        """Retrieve source systems by external ID.

        Args:
            items: The external IDs of the source systems to retrieve.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved source systems.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list(self, limit: int | None = None) -> builtins.list[ExtractorSourceSystemResponse]:
        """List source systems.

        Args:
            limit: Maximum number of source systems to return. None returns all.
        Returns:
            The source systems.
        """
        return self._list_catalog(limit)


class ExtractorsAPI(_ExtractorCatalogAPI[ExtractorResponse]):
    """API for the extractor catalog, with releases, solutions, and source systems as sub-APIs."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, "/extractors")
        self._schema_path = "/extractors/schemas/{extractor}/{version}"
        self.releases = ExtractorReleasesAPI(http_client)
        self.solutions = ExtractorSolutionsAPI(http_client)
        self.source_systems = ExtractorSourceSystemsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractorResponse]:
        return PagedResponse[ExtractorResponse].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ExtractorResponse]:
        """Retrieve extractors by external ID.

        Args:
            items: The external IDs of the extractors to retrieve.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved extractors.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list(self, limit: int | None = None) -> builtins.list[ExtractorResponse]:
        """List extractors.

        Args:
            limit: Maximum number of extractors to return. None returns all.
        Returns:
            The extractors.
        """
        return self._list_catalog(limit)

    def schema(self, extractor: str, version: str) -> dict[str, JsonValue]:
        """Get the configuration schema of an extractor release.

        Args:
            extractor: The external ID of the extractor.
            version: The release version, for example ``1.2.3``.
        Returns:
            The configuration schema as a JSON Schema document.
        """
        path = self._schema_path.format(extractor=quote(extractor, safe=""), version=quote(version, safe=""))
        request = RequestMessage(endpoint_url=self._make_url(path), method="GET")
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return TypeAdapter(dict[str, JsonValue]).validate_json(response.body)
