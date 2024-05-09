from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class WorkflowRunConfig:
    workflow_xid: str
    workflow_ver: int

    @classmethod
    def load(cls, data: dict[str, Any]) -> WorkflowRunConfig:
        return cls(
            workflow_xid=data["WorkflowExtId"],
            workflow_ver=data["WorkflowVersion"],
        )


def load_config_parameters(function_data: dict[str, Any]) -> WorkflowRunConfig:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    data = {}

    if "WorkflowExtId" not in function_data:
        raise ValueError("Missing key 'WorkflowExtId' in input data to the function")

    data["WorkflowExtId"] = function_data["WorkflowExtId"]
    data["WorkflowVersion"] = function_data["WorkflowVersion"]

    return WorkflowRunConfig.load(data)
