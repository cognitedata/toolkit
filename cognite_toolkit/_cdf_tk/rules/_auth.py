from collections.abc import Iterable

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.rules._base import ToolkitLocalRule

BASE_CODE = "AUTH"


class CheckDataSetMissing(ToolkitLocalRule):
    """
    Checks that resources that can be scoped to a dataset are scoped to a dataset.

    ## What it does
    For example, if you have a workflow, it checks that `dataSetExternalId` is specified.

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
    insight_type = Recommendation

    def validate(self) -> Iterable[Recommendation]:
        for resource, source_file in self._get_validated_resources_with_file():
            if "data_set_external_id" not in type(resource).model_fields:
                continue
            data_set_external_id = getattr(resource, "data_set_external_id", None)
            if data_set_external_id is None:
                yield Recommendation(
                    message=f"Missing data set external ID for {resource.as_id()!s} {source_file.resource_type!s}",
                    code=self.code,
                    fix=f"Add a dataset association to the {source_file.resource_type!s}.",
                )
