from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.cruds._resource_cruds import WorkflowCRUD
from cognite_toolkit._cdf_tk.resource_classes import WorkflowYAML
from cognite_toolkit._cdf_tk.rules._base import ToolkitResourceRule

BASE_CODE = "TOOLKIT-WORKFLOW"


class WorkflowDatasetMissing(ToolkitResourceRule[WorkflowYAML]):
    """
    Checks that workflows have a dataset association.

    ## What it does
    Verifies that each workflow has a `data_set_external_id` specified, associating it with a dataset.

    ## Why is this bad?
    The dataset ID identifies which dataset a workflow is associated with. A user must have access to this
    dataset to perform any actions on the workflow, such as viewing, updating, or deleting it. Additionally,
    to manage any resources connected to the workflow (such as triggers, versions, or executions), the user
    must also have access to the dataset. Without a dataset association, access control cannot be properly
    enforced, which can lead to permission issues and security concerns.

    ## Example
    **Bad**: Workflow without dataset association
    ```yaml
    externalId: my_workflow
    # Missing data_set_external_id
    ```

    **Good**: Workflow with dataset association
    ```yaml
    externalId: my_workflow
    dataSetExternalId: my_dataset
    ```
    """

    code = f"{BASE_CODE}-001"
    resource_type = WorkflowCRUD.kind
    insight_type = Recommendation

    def validate(self) -> list[Recommendation]:

        recommendations: list[Recommendation] = []

        for resource in self.resources:
            if not resource.data_set_external_id:
                recommendations.append(
                    Recommendation(
                        message=f"{resource.as_id()!s} is missing dataset association.",
                        code=self.code,
                        fix="Add dataset association to the workflow.",
                    )
                )

        return recommendations
