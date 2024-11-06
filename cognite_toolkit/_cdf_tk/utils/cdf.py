from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes.data_modeling import View, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock


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
