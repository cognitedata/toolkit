from pydantic import Field

from .base import ToolkitResource


class SimulatorRoutineYAML(ToolkitResource):
    """Simulator routine YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_simulators_routines_post
    """

    external_id: str = Field(description="External ID of the simulator routine.", min_length=1, max_length=255)
    model_external_id: str = Field(description="External ID of the simulator model.", min_length=1, max_length=255)
    simulator_integration_external_id: str = Field(
        description="External ID of the simulator integration.", min_length=1, max_length=255
    )
    name: str = Field(description="Name of the simulator routine.", min_length=1, max_length=255)
    description: str | None = Field(None, description="Description of the simulator routine.", max_length=500)
