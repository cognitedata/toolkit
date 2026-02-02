from typing import Any

from pydantic import Field

from .base import ToolkitResource


class SimulatorRoutineRevisionYAML(ToolkitResource):
    """Simulator routine revision YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post
    """

    external_id: str = Field(description="External ID of the simulator routine revision.", min_length=1, max_length=255)
    routine_external_id: str = Field(description="External ID of the simulator routine.", min_length=1, max_length=255)
    configuration: dict[str, Any] = Field(description="Configuration of the simulator routine revision.")
    script: list[dict[str, Any]] | None = Field(None, description="Script steps for the routine revision.")
