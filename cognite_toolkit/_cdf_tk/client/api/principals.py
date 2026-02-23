"""Principals API for managing CDF principals.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Principals
"""

from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import Literal

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType
from cognite_toolkit._cdf_tk.client.resource_classes.principal import (
    Principal,
)
from cognite_toolkit._cdf_tk.client.api.project import ProjectAPI

_PRINCIPAL_LIST_ADAPTER = TypeAdapter(list[Principal])


class PrincipalsAPI():
    """API for the Cognite Principals endpoints.

    Unlike most CDF APIs, the Principals API is not project-scoped.
    The `me` endpoint uses `/api/v1/principals/me`, and all other
    endpoints use `/api/v1/orgs/{org}/principals/...`.
    """

    def __init__(self, config: ToolkitClientConfig, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        self._config = config
        self._http_client = http_client
        self._project_api = project_api

    def me(self) -> Principal:
        """Get the current caller's principal information.

        Returns:
            The principal (ServiceAccountPrincipal or UserPrincipal) that issued the request.
        """
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._config.create_auth_url("/principals/me"),
                method="GET",
            )
        ).get_success_or_raise()
        return TypeAdapter(Principal).validate_json(response.body)

    def list_all(
        self,
        org: str,
        types: builtins.list[Literal["SERVICE_ACCOUNT", "USER"]] | None = None,
    ) -> builtins.list[Principal]:
        """List all principals in an organization, handling pagination automatically.

        Args:
            org: ID of the organization.
            types: Filter by principal types. If not specified, all types are included.

        Returns:
            A list of all principals in the organization.
        """
        all_items: builtins.list[Principal] = []
        cursor: str | None = None
        while True:
            page = self.list(org=org, types=types, limit=100, cursor=cursor)
            all_items.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return all_items

    def retrieve(
        self,
        org: str,
        items: Sequence[PrincipalReference | PrincipalExternalIdReference],
        ignore_unknown_ids: bool = False,
    ) -> builtins.list[Principal]:
        """Retrieve principals by ID or external ID.

        Service accounts can be retrieved by ID or external ID.
        Users can be retrieved by ID.

        Args:
            org: ID of the organization.
            items: List of principal references (by ID or external ID).
            ignore_unknown_ids: If True, IDs that do not match existing principals will be ignored.

        Returns:
            A list of matching principals.
        """
        body: dict[str, object] = {
            "items": [item.dump() for item in items],
        }
        if ignore_unknown_ids:
            body["ignoreUnknownIds"] = True

        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=f"{self._base_url()}/api/v1/orgs/{org}/principals/byids",
                method="POST",
                body_content=body,  # type: ignore[arg-type]
            )
        )
        success = response.get_success_or_raise()
        body_json = success.body_json
        return _PRINCIPAL_LIST_ADAPTER.validate_python(body_json.get("items", []))

    def list_sessions(
        self,
        org: str,
        principal: str,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[LoginSession]:
        """List login sessions for a user principal.

        This endpoint does not work for service account principals.

        Args:
            org: ID of the organization.
            principal: ID of the principal.
            limit: Maximum number of items to return (1-1000).
            cursor: Cursor for paging through results.

        Returns:
            A paged response containing login sessions and an optional next cursor.
        """
        params: dict[str, PrimitiveType] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor

        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=f"{self._base_url()}/api/v1/orgs/{org}/principals/{principal}/sessions",
                method="GET",
                parameters=params,
            )
        )
        success = response.get_success_or_raise()
        body = success.body_json
        items = [LoginSession.model_validate(item) for item in body.get("items", [])]
        next_cursor_obj = body.get("nextCursor")
        next_cursor = next_cursor_obj.get("cursor") if isinstance(next_cursor_obj, dict) else None
        return PagedResponse[LoginSession](items=items, nextCursor=next_cursor)

    def list_all_sessions(
        self,
        org: str,
        principal: str,
    ) -> builtins.list[LoginSession]:
        """List all login sessions for a user principal, handling pagination automatically.

        Args:
            org: ID of the organization.
            principal: ID of the principal.

        Returns:
            A list of all login sessions for the principal.
        """
        all_items: builtins.list[LoginSession] = []
        cursor: str | None = None
        while True:
            page = self.list_sessions(org=org, principal=principal, limit=1000, cursor=cursor)
            all_items.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return all_items

    def revoke_sessions(
        self,
        org: str,
        principal: str,
        items: Sequence[LoginSessionReference],
    ) -> None:
        """Revoke login sessions for a user principal.

        This endpoint is atomic: if revocation of any session fails, none are revoked.
        Does not work for service account principals.

        Args:
            org: ID of the organization.
            principal: ID of the principal.
            items: List of login session references to revoke (max 10 per call).
        """
        for i in range(0, len(items), 10):
            batch = items[i : i + 10]
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=f"{self._base_url()}/api/v1/orgs/{org}/principals/{principal}/sessions/revoke",
                    method="POST",
                    body_content={"items": [item.dump() for item in batch]},
                )
            )
            response.get_success_or_raise()

    def list(
        self,
        org: str,
        types: builtins.list[Literal["SERVICE_ACCOUNT", "USER"]] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[Principal]:
        """List principals in an organization.

        Args:
            org: ID of the organization.
            types: Filter by principal types. If not specified, all types are included.
            limit: Maximum number of items to return (1-100).
            cursor: Cursor for paging through results.

        Returns:
            A paged response containing the principals and an optional next cursor.
        """
        params: dict[str, PrimitiveType] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if types is not None:
            params["types"] = ",".join(types)

        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=f"{self._base_url()}/api/v1/orgs/{org}/principals",
                method="GET",
                parameters=params,
            )
        )
        success = response.get_success_or_raise()
        body = success.body_json
        items = _PRINCIPAL_LIST_ADAPTER.validate_python(body.get("items", []))
        next_cursor_obj = body.get("nextCursor")
        next_cursor = next_cursor_obj.get("cursor") if isinstance(next_cursor_obj, dict) else None
        return PagedResponse[Principal](items=items, nextCursor=next_cursor)
