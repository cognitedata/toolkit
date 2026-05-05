from __future__ import annotations

from pathlib import Path

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand

# ---------------------------------------------------------------------------
# Static mock YAML templates
# These will be replaced with dynamic translation from input rules later.
# ---------------------------------------------------------------------------

_WORKFLOW_YAML = """\
externalId: entity_matching_aliasing
description: Entity matching aliasing workflow
"""

_WORKFLOW_VERSION_YAML = """\
workflowExternalId: entity_matching_aliasing
version: 'v1'
workflowDefinition:
  description: 'Entity matching aliasing workflow'
  tasks:
    - externalId: configurator
      type: function
      name: Configurator
      retries: 0
      parameters:
        function:
          externalId: aliasing_aliasing_configurator
          data:
            path: /
            method: POST
      onFailure: abortWorkflow
      dependsOn: []

    - externalId: key_extraction
      type: jsonMapping
      name: Extract Keys from Properties
      retries: 0
      parameters:
        jsonMapping:
          expression: ${configurator.output.response.key_extraction_expression}
          inputs:
            - input
          input: ${workflow.input}
      onFailure: abortWorkflow
      dependsOn:
        - externalId: configurator

    - externalId: aliasing_task
      type: jsonMapping
      name: Aliasing
      retries: 0
      parameters:
        jsonMapping:
          expression: ${configurator.output.response.aliasing_expression}
          inputs:
            - input
          input: ${key_extraction.output.result}
      onFailure: abortWorkflow
      dependsOn:
        - externalId: key_extraction
"""


class EntityMatchingCommand(ToolkitCommand):
    """Commands for the entity-matching family."""

    def generate_aliasing_workflow(
        self,
        input_yaml: Path,
        output_dir: Path,
    ) -> None:
        """Generate Workflow and WorkflowVersion YAMLs from an aliasing-rules input file.

        Currently writes static mock files. Future iterations will translate
        rules from the input YAML into the workflow task definitions.
        """
        if not input_yaml.exists():
            raise FileNotFoundError(f"Input file not found: {input_yaml}")

        output_dir.mkdir(parents=True, exist_ok=True)

        stem = input_yaml.stem

        workflow_path = output_dir / f"{stem}.Workflow.yaml"
        workflow_version_path = output_dir / f"{stem}.WorkflowVersion.yaml"

        workflow_path.write_text(_WORKFLOW_YAML, encoding="utf-8")
        self.console(f"Generated {workflow_path.as_posix()}")

        workflow_version_path.write_text(_WORKFLOW_VERSION_YAML, encoding="utf-8")
        self.console(f"Generated {workflow_version_path.as_posix()}")

        print(Panel(f"Generated 2 files in {output_dir.as_posix()}", title="Success", style="green", expand=False))
