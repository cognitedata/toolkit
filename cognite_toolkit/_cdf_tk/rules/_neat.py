from collections.abc import Iterable
from functools import cached_property
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    Insight,
    ModelSyntaxWarning,
    Recommendation,
)
from cognite_toolkit._cdf_tk.cruds import DataModelIO

from ._base import FailedValidation, RuleSetStatus, ToolkitGlobalRulSet

if TYPE_CHECKING:
    from cognite.neat._toolkit_adapter import NeatClient, NeatIssueList, SchemaLimits, SchemaSnapshot


class NeatRuleSet(ToolkitGlobalRulSet):
    CODE_PREFIX = "NEAT"
    DISPLAY_NAME = "Neat (data modeling)"

    def get_status(self) -> RuleSetStatus:
        is_installed = self.installed()
        if is_installed and self.client:
            return RuleSetStatus(
                code="ready",
                message="Neat is installed and will be used to validate data models. This validation may take a while since it needs to fetch the entire CDF snapshot.",
            )
        missing: list[str] = []
        if not is_installed:
            missing.append("Neat is not installed. Install with `pip install cognite-neat`.")
        if not self.client:
            missing.append("Neat requires a client. Provide client credentials to use Neat for validation.")
        message = "Neat is unavailable. " + " ".join(missing)
        return RuleSetStatus(code="unavailable", message=message)

    def validate(self) -> Iterable[Insight | FailedValidation]:
        data_model_type = ResourceType(resource_folder=DataModelIO.folder_name, kind=DataModelIO.kind)
        for module in self.modules:
            for resource in module.resources:
                if resource.type == data_model_type:
                    data_model_file = resource.build_path
                    try:
                        yield from self._validate_model(data_model_file.parent, data_model_file)
                    except Exception as e:
                        yield FailedValidation(
                            message=f"Neat plugin failed to validate data model {data_model_file.name!r}: {e}",
                            source=str(resource.identifier),
                        )

    @classmethod
    def installed(cls) -> bool:
        """Check if neat is installed"""
        return find_spec("cognite.neat") is not None

    def _validate_model(self, data_model_dir: Path, data_model_file: Path) -> Iterable[Insight]:
        """Validates a data model using Neat and returns a list of insights.

        Args:
            data_model_dir: The directory containing the data model YAML file.
            data_model_file: The data model YAML file to validate.

        Returns:
            InsightList: A list of insights generated from the validation.

        Note:
            NEAT should be ran in "rebuild" mode since toolkit is designed to build and deploy artifacts which are
            defined in Toolkit modules.
        """

        from cognite.neat._toolkit_adapter import DMSAPIImporter, DmsDataModelRulesOrchestrator

        importer = DMSAPIImporter.from_yaml(yaml_file=data_model_dir, data_model_file=data_model_file)
        schema = importer.to_data_model()

        orchestrator = DmsDataModelRulesOrchestrator(
            cdf_snapshot=self._cdf_snapshot, limits=self._cdf_limits, modus_operandi="rebuild"
        )
        orchestrator.run(schema)

        yield from self.issues_to_insights(orchestrator.issues)

    @classmethod
    def issues_to_insights(cls, issues: "NeatIssueList") -> Iterable[Insight]:
        """Converts a list of Neat issues to a Toolkit insight list.

        Args:
            issues: List of Neat issues.

        Returns:
            InsightList: List of Toolkit insights.
        """
        from cognite.neat._toolkit_adapter import NeatConsistencyError, NeatModelSyntaxError, NeatRecommendation

        for issue in issues:
            if isinstance(issue, NeatModelSyntaxError):
                yield ModelSyntaxWarning.model_validate(issue.model_dump())
            elif isinstance(issue, NeatRecommendation):
                yield Recommendation.model_validate(issue.model_dump())
            elif isinstance(issue, NeatConsistencyError):
                yield ConsistencyError.model_validate(issue.model_dump())

    @cached_property
    def _neat_client(self) -> "NeatClient":
        if self.client is None:
            raise RuntimeError(
                "NeatRules requires a client to be provided to fetch CDF snapshot and limits for validation. Please provide client credentials."
            )
        return NeatClient(self.client._config)

    @cached_property
    def _cdf_limits(self) -> "SchemaLimits":
        from cognite.neat._toolkit_adapter import SchemaLimits

        return SchemaLimits.from_api_response(self._neat_client.statistics.project())

    @cached_property
    def _cdf_snapshot(self) -> "SchemaSnapshot":
        from cognite.neat._data_model._snapshot import SchemaSnapshot

        return SchemaSnapshot.fetch_entire_cdf(self._neat_client)
