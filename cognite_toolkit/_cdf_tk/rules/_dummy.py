from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD

from ._base import ToolkitRule


class DummyDataModelRule(ToolkitRule):
    code = "DUMMY_MODEL_RULE"
    resource_type = DataModelCRUD.kind
    insight_type = Recommendation

    def validate(self) -> list[Recommendation]:  # Replace with actual return type
        # Implement validation logic here
        return [Recommendation(message="This is a dummy rule. Replace with actual validation logic.")]
