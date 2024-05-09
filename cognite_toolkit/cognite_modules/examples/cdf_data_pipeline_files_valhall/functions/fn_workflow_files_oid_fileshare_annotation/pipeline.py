from __future__ import annotations

import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes import ClientCredentials
from config import WorkflowRunConfig


@dataclass
class Entity:
    external_id: str
    org_name: str
    name: list[str]
    id: int
    type: str = "file"

    def dump(self) -> dict[str, Any]:
        return {
            "externalId": self.external_id,
            "orgName": self.org_name,
            "name": self.name,
            "id": self.id,
            "type": self.type,
        }


def run_workflow(client: CogniteClient, config: WorkflowRunConfig, secrets: dict) -> None:
    """
    Read configuration and start Workflow

    Args:
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """

    workflow_log_msg = f"Workflow for with external ID : {config.workflow_xid} and version : {config.workflow_ver}"

    try:
        t_end = time.time() + 60 * 8

        res = client.workflows.executions.trigger(
            config.workflow_xid,
            config.workflow_ver,
            client_credentials=ClientCredentials(secrets["client-id"], secrets["client-secret"]),
        )

        workflow_execution_id = res.id

        start_time = datetime.fromtimestamp(int(res.start_time / 1000), timezone.utc).strftime("%d %b %Y %H:%M:%S")
        msg = f"{workflow_log_msg} started: {start_time}  status: {res.status}."

        while time.time() < t_end:
            res = client.workflows.executions.retrieve_detailed(workflow_execution_id)

            if res.status != "running":
                break

            # wait for 5 seconds before checking again
            time.sleep(5)

        if res.status == "running":
            msg = f"{workflow_log_msg} function timing out - not able to wait for completion of workflow .... se workflow UI for status."
            print(f"[INFO] {msg}")
        elif res.status == "completed":
            end_time = datetime.fromtimestamp(int(res.end_time / 1000), timezone.utc).strftime("%d %b %Y %H:%M:%S")
            msg = f"{workflow_log_msg} finished at : {end_time} status: {res.status}."
            print(f"[INFO] {msg}")
        else:
            msg = f"{workflow_log_msg} failed with status: {res.status} - error message: {res.reason_for_incompletion}"
            print(f"[ERROR] {msg}")

    except Exception as e:
        msg = f"{workflow_log_msg} executed failed. Message: {e!s}, traceback:\n{traceback.format_exc()}"
        print(f"[ERROR] {msg}")
