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
        aliasing_task_ids: list[str] = []

        for rule, kuiper in rule_kuiper_pairs:
            expression = kuiper.expression
            external_id = f"aliasing_task_{rule.name}"
            aliasing_task_ids.append(external_id)
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

        if len(aliasing_task_ids) > 1:
            combiner_task_yaml = self._build_combiner_task(aliasing_task_ids)
            tasks_yaml += combiner_task_yaml + "\n"

        return f"""\
workflowExternalId: {workflow_external_id}
version: v1
workflowDefinition:
  description: Entity matching aliasing workflow
  tasks:
{tasks_yaml}"""

    def _build_combiner_task(self, aliasing_task_ids: list[str]) -> str:
        kuiper_expression = self._get_combiner_kuiper_expression()
        escaped_expression = self._escape_expression(kuiper_expression)
        aliasing_task_results = self._build_aliasing_task_results_string(aliasing_task_ids)
        depends_on = self._build_dependencies_string(aliasing_task_ids)

        return f"""\
    - externalId: combiner_task
      type: jsonMapping
      name: Combiner
      description: Combines results from all aliasing tasks
      dependsOn:
      {depends_on}
      retries: 0
      parameters:
        jsonMapping:
          expression: "{escaped_expression}"
          inputs:
            - input
          input: {{
              "aliasing_task_results": [
                {aliasing_task_results}
              ]
            }}
      onFailure: abortWorkflow"""

    def _get_combiner_kuiper_expression(self) -> str:
        return '#get_external_ids := (aliasing_task_results) => aliasing_task_results.flatmap(results => results.map(item => { "external_id": item.external_id, "space": item.space })).distinct_by(obj => obj); #get_aliases_for_group := (aliasing_task_results, group) => aliasing_task_results.flatmap(results => results).filter(result => result.external_id == group.external_id && result.space == group.space).flatmap(result => result.aliases).distinct_by(x => x); get_external_ids(input.aliasing_task_results).map(grouping => { "external_id": grouping.external_id, "space": grouping.space, "aliases": get_aliases_for_group(input.aliasing_task_results, grouping) })'

    def _escape_expression(self, expression: str) -> str:
        return expression.replace('"', '\\"')

    def _build_aliasing_task_results_string(self, aliasing_task_ids: list[str]) -> str:
        return ",\n                ".join(f'"${{{task_id}.output.result}}"' for task_id in aliasing_task_ids)

    def _build_dependencies_string(self, aliasing_task_ids: list[str]) -> str:
        return "\n      ".join(f"- externalId: {task_id}" for task_id in aliasing_task_ids)
