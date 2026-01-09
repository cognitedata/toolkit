from pydantic import Field

from .base import ToolkitResource


class SimulatorModelYAML(ToolkitResource):
    """Simulator model YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/create_simulator_model_simulators_models_post
    """

    external_id: str = Field(description="External ID of the simulator model.", min_length=1, max_length=255)
    simulator_external_id: str = Field(description="External id of the simulator.", min_length=1, max_length=50)
    name: str = Field(description="The name of the simulator model.", min_length=1, max_length=50)
    description: str | None = Field(None, description="Description of the simulator model.", max_length=500)
    data_set_external_id: str = Field(description="The external ID of the dataset this simulator model belongs to.")
    type: str = Field(description="The type of the simulator model.", min_length=1, max_length=50)
