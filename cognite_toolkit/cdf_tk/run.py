from __future__ import annotations

import datetime
import json
import time
from typing import Any

from cognite.client.data_classes import CreatedSession, FunctionCall
from cognite.client.data_classes.transformations import TransformationList
from cognite.client.data_classes.transformations.common import NonceCredentials
from rich import print
from rich.table import Table

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


def run_function(ToolGlobals: CDFToolConfig, external_id: str, payload: str, follow: bool = False) -> bool:
    """Run a function in CDF"""
    session = get_oneshot_session(ToolGlobals)
    if session is None:
        print("[bold red]ERROR:[/] Could not get a oneshot session.")
        return False
    try:
        function = ToolGlobals.client.functions.retrieve(external_id=external_id)
    except Exception as e:
        print("[bold red]ERROR:[/] Could not retrieve function.")
        print(e)
        return False
    if function is None:
        print(f"[bold red]ERROR:[/] Could not find function with external_id {external_id}")
        return False
    try:
        data: dict[str, Any] = json.loads(payload or "{}")
    except Exception as e:
        print("[bold red]ERROR:[/] Could not parse payload.")
        print(e)
        return False

    def _function_call(id: int, payload: dict[str, Any]) -> FunctionCall | None:
        (_, bearer) = ToolGlobals.oauth_credentials.authorization_header()
        session = get_oneshot_session(ToolGlobals)
        if session is None:
            print("[bold red]ERROR:[/] Could not get a oneshot session.")
            return None
        nonce = session.nonce
        ret = ToolGlobals.client.post(
            url=f"/api/v1/projects/{ToolGlobals.project}/functions/{id}/call",
            json={
                "data": payload,
                "nonce": nonce,
            },
            headers={"Authorization": bearer},
        )
        if ret.status_code == 201:
            return FunctionCall.load(ret.json())
        return None

    try:
        call_result = _function_call(id=function.id, payload=data)
        if call_result is None:
            print("[bold red]ERROR:[/] Could not run function.")
            return False
        table = Table(title=f"Function {external_id}, id {function.id}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Call id", str(call_result.id))
        table.add_row("Status", str(call_result.status))
        table.add_row("Created time", str(datetime.datetime.fromtimestamp((call_result.start_time or 1000) / 1000)))
        print(table)
    except Exception as e:
        print("[bold red]ERROR:[/] Could not run function.")
        print(e)
        return False

    if follow:
        print("Awaiting results from function call...")
        sleep_time = 1
        while True:
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, 60)
            call_result = ToolGlobals.client.functions.calls.retrieve(
                call_id=call_result.id or 0, function_id=function.id
            )
            if call_result is None:
                print("[bold red]ERROR:[/] Could not retrieve function call result.")
                return False
            if call_result.status != "Running":
                break
        table = Table(title=f"Function {external_id}, id {function.id}")
        table.add_column("Info", justify="left")
        table.add_column("Value", justify="left", style="green")
        table.add_row("Call id", str(call_result.id))
        table.add_row("Status", str(call_result.status))
        table.add_row("Created time", str(datetime.datetime.fromtimestamp((call_result.start_time or 1000) / 1000)))
        table.add_row("Finished time", str(datetime.datetime.fromtimestamp((call_result.end_time or 1000) / 1000)))
        table.add_row("Duration", str((call_result.end_time or 1) - (call_result.start_time or 1)))
        if call_result.error is not None:
            table.add_row("Error", str(call_result.error.get("message", "Empty error")))
            table.add_row("Error trace", str(call_result.error.get("trace", "Empty trace")))
        result = ToolGlobals.client.functions.calls.get_response(call_id=call_result.id or 0, function_id=function.id)
        table.add_row("Result", str(json.dumps(result, indent=2, sort_keys=True)))
        logs = ToolGlobals.client.functions.calls.get_logs(call_id=call_result.id or 0, function_id=function.id)
        table.add_row("Logs", str(logs))
        print(table)
    return True


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
