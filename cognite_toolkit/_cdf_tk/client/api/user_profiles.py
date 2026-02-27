"""User Profiles API for managing CDF user profiles.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/User-profiles

All endpoints in this API are deprecated in favor of the Principals API.
"""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import UserProfileId
from cognite_toolkit._cdf_tk.client.resource_classes.user_profile import UserProfile

IdentityTypeFilter = Literal["ALL", "USER", "SERVICE_PRINCIPAL", "INTERNAL_SERVICE"]


class UserProfilesAPI(CDFResourceAPI[UserProfile]):
    """API for the Cognite User Profiles endpoints.

    All endpoints are project-scoped and deprecated in favor of the Principals API.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/profiles", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/profiles/byids", item_limit=1000),
            },
        )
        self._me_endpoint = Endpoint(method="GET", path="/profiles/me", item_limit=1)
        self._search_endpoint = Endpoint(method="POST", path="/profiles/search", item_limit=1000)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[UserProfile]:
        return PagedResponse[UserProfile].model_validate_json(response.body)

    def me(self) -> UserProfile:
        """Get the user profile of the principal issuing the request.

        Returns:
            The UserProfile of the requesting principal.
        """
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(self._me_endpoint.path),
                method=self._me_endpoint.method,
            )
        ).get_success_or_raise()
        return UserProfile.model_validate_json(response.body)

    def retrieve(self, items: Sequence[UserProfileId]) -> list[UserProfile]:
        """Retrieve user profiles by their user identifiers.

        Args:
            items: A sequence of UserProfileId objects to retrieve.
        """
        return self._request_item_response(items, "retrieve")

    def search(self, name: str, limit: int = 25) -> list[UserProfile]:
        """Search user profiles in the current project.

        Args:
            name: The name to search for.
            limit: Maximum number of results. Default is 25.

        Returns:
            A list of matching UserProfile objects.
        """
        if limit < 1 or limit > self._search_endpoint.item_limit:
            raise ValueError(f"Limit must be between 1 and {self._search_endpoint.item_limit}")
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(self._search_endpoint.path),
                method=self._search_endpoint.method,
                body_content={"search": {"name": name}, "limit": limit},
            )
        ).get_success_or_raise()
        return self._validate_page_response(response).items

    def paginate(
        self,
        identity_type: IdentityTypeFilter | None = None,
        limit: int = 25,
        cursor: str | None = None,
    ) -> PagedResponse[UserProfile]:
        """Paginate through all user profiles in the current project.

        Args:
            identity_type: Filter by identity type. Default returns USER profiles.
            limit: Maximum number of items per page. Default is 25.
            cursor: Cursor for pagination.

        Returns:
            A PagedResponse containing UserProfile objects and a next cursor.
        """
        params = self._create_list_parameters(identity_type)
        return self._paginate(limit, cursor, params=params)

    def iterate(
        self,
        identity_type: IdentityTypeFilter | None = None,
        limit: int | None = 25,
        cursor: str | None = None,
    ) -> Iterable[list[UserProfile]]:
        """Iterate through all user profiles in the current project."""
        params = self._create_list_parameters(identity_type)
        return self._iterate(params=params, limit=limit, cursor=cursor)

    def list(
        self,
        identity_type: IdentityTypeFilter | None = None,
        limit: int | None = 25,
    ) -> list[UserProfile]:
        """List user profiles in the current project.

        Args:
            identity_type: Filter by identity type. Default returns USER profiles.
            limit: Maximum number of items to return. Default is 25.

        Returns:
            A list of UserProfile objects.
        """
        params = self._create_list_parameters(identity_type)
        return self._list(params=params, limit=limit)

    @staticmethod
    def _create_list_parameters(identity_type: IdentityTypeFilter | None) -> dict[str, str] | None:
        if identity_type is None:
            return {"identityType": "ALL"}
        return {"identityType": identity_type}
