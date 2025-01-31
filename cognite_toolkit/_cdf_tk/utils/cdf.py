from collections.abc import Iterator
from typing import Any, Literal, overload

from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes.data_modeling import Edge, Node, View, ViewId
from cognite.client.data_classes.filters import SpaceFilter
from cognite.client.exceptions import CogniteAPIError
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning


def retrieve_view_ancestors(client: ToolkitClient, parents: list[ViewId], cache: dict[ViewId, View]) -> list[View]:
    """Retrieves all ancestors of a view.

    This will mutate the cache that is passed in, and return a list of views that are the ancestors of the views in the parents list.

    Args:
        client: The Cognite client to use for the requests
        parents: The parents of the view to retrieve all ancestors for
        cache: The cache to store the views in
    """
    parent_ids = parents
    found: list[View] = []
    while parent_ids:
        to_lookup = []
        grand_parent_ids = []
        for parent in parent_ids:
            if parent in cache:
                found.append(cache[parent])
                grand_parent_ids.extend(cache[parent].implements or [])
            else:
                to_lookup.append(parent)

        if to_lookup:
            looked_up = client.data_modeling.views.retrieve(to_lookup)
            cache.update({view.as_id(): view for view in looked_up})
            found.extend(looked_up)
            for view in looked_up:
                grand_parent_ids.extend(view.implements or [])

        parent_ids = grand_parent_ids
    return found


def get_oneshot_session(client: ToolkitClient) -> CreatedSession | None:
    """Get a oneshot (use once) session for execution in CDF"""
    # Special case as this utility function may be called with a new client created in code,
    # it's hard to mock it in tests.
    if isinstance(client, ToolkitClientMock):
        bearer = "123"
    else:
        (_, bearer) = client.config.credentials.authorization_header()
    ret = client.post(
        url=f"/api/v1/projects/{client.config.project}/sessions",
        json={
            "items": [
                {
                    "oneshotTokenExchange": True,
                },
            ],
        },
        headers={"Authorization": bearer},
    )
    if ret.status_code == 200:
        return CreatedSession.load(ret.json()["items"][0])
    return None


@overload
def iterate_instances(
    client: ToolkitClient,
    instance_type: Literal["node"] = "node",
    space: str | None = None,
    console: Console | None = None,
) -> Iterator[Node]: ...


@overload
def iterate_instances(
    client: ToolkitClient, instance_type: Literal["edge"], space: str | None = None, console: Console | None = None
) -> Iterator[Edge]: ...


def iterate_instances(
    client: ToolkitClient,
    instance_type: Literal["node", "edge"] = "node",
    space: str | None = None,
    console: Console | None = None,
) -> Iterator[Node] | Iterator[Edge]:
    """Toolkit specific implementation of the client.data_modeling.instances(...) method to account for 408.

    In addition, we enforce sort based on the argument below (provided by Alex B.).
    """
    body: dict[str, Any] = {"limit": 1_000, "cursor": None, "instanceType": instance_type}
    # Without a sort, the sort is implicitly by the internal id, as cursoring needs a stable sort.
    # By making the sort be on external_id, Postgres should pick the index that's on (project_id, space, external_id)
    # WHERE deleted_at IS NULL. In other words, avoiding soft deleted instances.
    body["sort"] = [
        {
            "property": [instance_type, "externalId"],
            "direction": "ascending",
        }
    ]
    url = f"/api/{client._API_VERSION}/projects/{client.config.project}/models/instances/list"
    if space:
        body["filter"] = SpaceFilter(space=space, instance_type=instance_type).dump()
    while True:
        try:
            response = client.post(url=url, json=body)
        except CogniteAPIError as e:
            if e.code == 408 and body["limit"] > 1:
                MediumSeverityWarning(
                    f"Timeout with limit {body['limit']}, retrying with {body['limit'] // 2}."
                ).print_warning(include_timestamp=True, console=console)
                body["limit"] = body["limit"] // 2
                continue
            raise e
        response_body = response.json()
        if instance_type == "node":
            yield from (Node.load(node) for node in response_body["items"])
        else:
            yield from (Edge.load(edge) for edge in response_body["items"])
        next_cursor = response_body.get("nextCursor")
        if next_cursor is None:
            break
        body["cursor"] = next_cursor
