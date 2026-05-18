from dataclasses import dataclass

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule


@dataclass(frozen=True)
class WorkflowBundle:
    workflow_yaml: str
    workflow_version_yaml: str


class WorkflowVersionAssembly:
    def build(
        self,
        rule_kuiper_pairs: list[tuple[AliasingRule, AliasingKuiper]],
        workflow_external_id: str | None = None,
        workflow_description: str | None = None,
    ) -> WorkflowBundle:
        workflow_yaml = self._build_workflow(workflow_external_id, workflow_description)
        workflow_version_yaml = self._build_workflow_version(rule_kuiper_pairs, workflow_external_id)
        return WorkflowBundle(workflow_yaml=workflow_yaml, workflow_version_yaml=workflow_version_yaml)

    def _build_workflow(self, external_id: str | None = None, description: str | None = None) -> str:
        external_id = external_id or "entity_matching_aliasing"
        description = description or "Entity matching aliasing workflow"
        return f"""\
externalId: {external_id}
description: {description}
"""

    def _build_workflow_version(
        self,
        rule_kuiper_pairs: list[tuple[AliasingRule, AliasingKuiper]],
        workflow_external_id: str | None = None,
    ) -> str:
        workflow_external_id = workflow_external_id or "entity_matching_aliasing"
        tasks_yaml = ""

        for rule, kuiper in rule_kuiper_pairs:
            expression = kuiper.expression
            external_id = f"aliasing_task_{rule.name}"
            escaped_expression = expression.replace('"', '\\"')

            task_yaml = f"""\
    - externalId: {external_id}
      type: jsonMapping
      name: {rule.name}
      description: {rule.description}
      retries: 0
      parameters:
        jsonMapping:
          expression: "{escaped_expression}"
          inputs:
            - input
          input: ${{workflow.input}}
      onFailure: abortWorkflow"""

            tasks_yaml += task_yaml + "\n"

        return f"""\
workflowExternalId: {workflow_external_id}
version: v1
workflowDefinition:
  description: Entity matching aliasing workflow
  tasks:
{tasks_yaml}"""
