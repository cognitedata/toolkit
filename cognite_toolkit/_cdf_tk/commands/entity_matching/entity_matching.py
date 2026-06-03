from pathlib import Path

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.bootstrap.bootstrapper import (
    provide_aliasing_facade,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.workflow_assembly import (
    WorkflowVersionAssembly,
    WorkflowVersionAssemblyRequest,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.yaml_rules_reader import YamlRulesReader
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.utils.module_resolver import ModuleResolver


class EntityMatchingCommand(ToolkitCommand):
    """Commands for the entity-matching family."""

    def generate_aliasing_workflow(
        self,
        input_yaml: Path,
        module_name: str | None,
        organization_dir: Path,
    ) -> None:
        """
        Generate Workflow and WorkflowVersion YAMLs into a module's workflows/ folder.

        Parses aliasing rules from the input YAML file and generates a separate
        kuiper expression for each rule, creating workflow tasks accordingly.

        Args:
            input_yaml: Path to the aliasing-rules input file.
            module_name: Name of the module to write the workflow files into.
            organization_dir: Path to the organization directory.
        """
        if not input_yaml.exists():
            raise ToolkitFileNotFoundError(f"Input file not found: {input_yaml}")

        module_path = ModuleResolver.get_or_prompt_module_path(organization_dir, module_name)

        output_dir = module_path / "workflows"
        output_dir.mkdir(parents=True, exist_ok=True)

        stem = input_yaml.stem

        workflow_path = output_dir / f"{stem}.Workflow.yaml"
        workflow_version_path = output_dir / f"{stem}.WorkflowVersion.yaml"

        rules_reader = YamlRulesReader()
        rules_content = rules_reader.read_file(str(input_yaml))

        facade = provide_aliasing_facade()
        rule_kuiper_pairs = []
        for rule in rules_content.rules:
            kuiper = facade.generate([rule])
            rule_kuiper_pairs.append((rule, kuiper))
            self.console(f"Generated kuiper expression for rule '{rule.name}'")

        assembly = WorkflowVersionAssembly()
        bundle = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=rule_kuiper_pairs,
                key_path=rules_content.key_path,
                workflow_external_id=rules_content.workflow_id,
                workflow_description=rules_content.description,
            )
        )
        workflow_path.write_text(bundle.workflow_yaml, encoding="utf-8")
        self.console(f"Generated {workflow_path.as_posix()}")

        workflow_version_path.write_text(bundle.workflow_version_yaml, encoding="utf-8")
        self.console(f"Generated {workflow_version_path.as_posix()}")

        print(Panel(f"Generated 2 files in {output_dir.as_posix()}", title="Success", style="green", expand=False))
