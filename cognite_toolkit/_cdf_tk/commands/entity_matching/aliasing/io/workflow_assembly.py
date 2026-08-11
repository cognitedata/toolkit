from dataclasses import dataclass

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule


@dataclass(frozen=True)
class WorkflowBundle:
    workflow_yaml: str
    workflow_version_yaml: str


@dataclass(frozen=True)
class WorkflowVersionAssemblyRequest:
    rule_kuiper_pairs: list[tuple[AliasingRule, AliasingKuiper]]
    key_path: str
    workflow_external_id: str = "entity_matching_aliasing"
    workflow_description: str = "Entity matching aliasing workflow"


class WorkflowVersionAssembly:
    def build(self, request: WorkflowVersionAssemblyRequest) -> WorkflowBundle:
        workflow_yaml = self._build_workflow(request.workflow_external_id, request.workflow_description)
        workflow_version_yaml = self._build_workflow_version(
            request.rule_kuiper_pairs, request.key_path, request.workflow_external_id
        )
        return WorkflowBundle(workflow_yaml=workflow_yaml, workflow_version_yaml=workflow_version_yaml)

    def _build_workflow(self, external_id: str, description: str) -> str:
        return f"""\
externalId: {external_id}
description: {description}
"""

    def _build_workflow_version(
        self,
        rule_kuiper_pairs: list[tuple[AliasingRule, AliasingKuiper]],
        key_path: str,
        workflow_external_id: str = "entity_matching_aliasing",
    ) -> str:
        tasks_yaml = ""
        aliasing_task_ids: list[str] = []

        key_extraction_task_yaml = self._build_key_extraction_task(key_path)
        tasks_yaml += key_extraction_task_yaml + "\n"

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
      dependsOn:
        - externalId: key_extraction_task
      retries: 0
      parameters:
        jsonMapping:
          expression: "{escaped_expression}"
          inputs:
            - input
          input: ${{key_extraction_task.output.result}}
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

    def _build_key_extraction_task(self, key_path: str) -> str:
        expression = f'{{ "keys": input.nodes.map(e => ({{"external_id": e.externalId, "space": e.space, "keys": [e.{key_path}]}}))}}'
        escaped_expression = expression.replace('"', '\\"')

        return f"""\
    - externalId: key_extraction_task
      type: jsonMapping
      name: Key Extraction
      description: Extracts keys from input nodes
      retries: 0
      parameters:
        jsonMapping:
          expression: "{escaped_expression}"
          inputs:
            - input
          input: ${{workflow.input}}
      onFailure: abortWorkflow"""

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
