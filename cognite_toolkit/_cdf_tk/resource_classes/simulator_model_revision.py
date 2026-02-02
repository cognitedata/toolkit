from pydantic import Field

from .base import ToolkitResource


class SimulatorModelRevisionYAML(ToolkitResource):
    """Simulator model revision YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post
    """

    external_id: str = Field(description="External ID of the simulator model revision.", min_length=1, max_length=255)
    model_external_id: str = Field(description="External ID of the simulator model.")
    description: str | None = Field(None, description="Description of the simulator model revision.", max_length=255)
    file_external_id: str = Field(description="External ID of the file containing the model.")
