from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import DataModelCRUD
from cognite_toolkit._cdf_tk.rules._base import ToolkitResourceRule


class DummyDataModelRule(ToolkitResourceRule):
    """This is a dummy rule for demonstration purposes.
    It does not perform any actual validation but serves as a template for implementing real rules.

    ## What it does

    ## Why is this bad?

    ## Example

    """

    code = "DUMMY_MODEL_RULE"
    resource_type = DataModelCRUD.kind
    insight_type = Recommendation

    def validate(self) -> list[Recommendation]:  # Replace with actual return type
        # Implement validation logic here
        return [
            Recommendation(
                message="This is a dummy rule. Replace with actual validation logic.",
                code=self.code,
                fix="Toolkit team to add an actual rule",
            )
        ]
