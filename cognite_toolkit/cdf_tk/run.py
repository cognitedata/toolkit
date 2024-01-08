from __future__ import annotations

from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes.transformations import TransformationList
from cognite.client.data_classes.transformations.common import NonceCredentials
from rich import print

from .utils import CDFToolConfig


def get_oneshot_session(ToolGlobals: CDFToolConfig) -> CreatedSession | None:
    """Get a oneshot (use once) session for execution in CDF"""
    ToolGlobals.verify_client(capabilities={"sessionsAcl": ["LIST", "CREATE", "DELETE"]})
    (_, bearer) = ToolGlobals.oauth_credentials.authorization_header()
    ret = ToolGlobals.client.post(
        url=f"/api/v1/projects/{ToolGlobals.project}/sessions",
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


def run_transformation(ToolGlobals: CDFToolConfig, external_ids: str | list[str]) -> bool:
    """Run a transformation in CDF"""
    if isinstance(external_ids, str):
        external_ids = [external_ids]
    session = get_oneshot_session(ToolGlobals)
    if session is None:
        print("[bold red]ERROR:[/] Could not get a oneshot session.")
        return False
    try:
        transformations: TransformationList = ToolGlobals.client.transformations.retrieve_multiple(
            external_ids=external_ids
        )
    except Exception as e:
        print("[bold red]ERROR:[/] Could not retrieve transformations.")
        print(e)
        return False
    if transformations is None or len(transformations) == 0:
        print(f"[bold red]ERROR:[/] Could not find transformation with external_id {external_ids}")
        return False
    nonce = NonceCredentials(session_id=session.id, nonce=session.nonce, cdf_project_name=ToolGlobals.project)
    for transformation in transformations:
        transformation.source_nonce = nonce
        transformation.destination_nonce = nonce
    try:
        ToolGlobals.client.transformations.update(transformations)
    except Exception as e:
        print("[bold red]ERROR:[/] Could not update transformations with oneshot session.")
        print(e)
        return False
    for transformation in transformations:
        try:
            job = ToolGlobals.client.transformations.run(
                transformation_external_id=transformation.external_id, wait=False
            )
            print(f"Running transformation {transformation.external_id}, status {job.status}...")
        except Exception as e:
            print(f"[bold red]ERROR:[/] Could not run transformation {transformation.external_id}.")
            print(e)
    return True
