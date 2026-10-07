from collections.abc import Iterable

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.rules._base import ToolkitLocalRule, quote_identifier


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

    CODE = "RESOURCE-UNGOVERNED"
    TITLE = "Ungoverned resource"
    insight_type = Recommendation

    def validate(self) -> Iterable[Recommendation]:
        for resource, source_file in self._get_validated_resources_with_file():
            if "data_set_external_id" not in type(resource).model_fields:
                continue
            data_set_external_id = getattr(resource, "data_set_external_id", None)
            # Some resources (e.g. functions) can be scoped to a space instead of a dataset, so we also check for space,
            # But only if the resource actually has a space field.
            supports_space = "space" in type(resource).model_fields
            space = getattr(resource, "space", None) if supports_space else None
            if data_set_external_id is None and space is None:
                kind = source_file.resource_type.kind
                if supports_space:
                    message = f"{kind} {quote_identifier(resource.as_id())} is not assigned to a data set or space."
                    fix = f"Set 'dataSetExternalId' or 'space' on the {kind}."
                else:
                    message = f"{kind} {quote_identifier(resource.as_id())} is not assigned to a data set."
                    fix = f"Set 'dataSetExternalId' on the {kind}."
                yield Recommendation(
                    message=message,
                    code=self.CODE,
                    title=self.TITLE,
                    fix=fix,
                    source_file=source_file.source_path,
                )
