from ._base import ToolkitGlobalRulSet, ToolkitRule


class NeatRules(ToolkitRule):
    code = "NEAT"

    def __init__(self):

    def _global_validation(self, built_modules: list[BuiltModule], client: ToolkitClient | None) -> InsightList:
        """This validation is performed per resource type and not per individual resource and against CDF
        for all modules. This validation will leverage external plugins such as NEAT.
        """
        # Can be parallelized with number of plugins.
        # Neat is done inside the global validation.
        insights = InsightList()
        for built_module in built_modules:
            if not built_module.files_built:
                continue
            data_model_type = ResourceType(resource_folder=DataModelCRUD.folder_name, kind=DataModelCRUD.kind)
            if data_model_files := built_module.resource_by_type_by_kind.get(data_model_type):
                if NeatPlugin.installed() and client and data_model_files:
                    neat = NeatPlugin(client)
                    for data_model_file in data_model_files:
                        try:
                            for insight in neat.validate(data_model_file.parent, data_model_file):
                                if insight not in built_module.insights:
                                    insights.append(insight)
                        except Exception as e:
                            self.warn(
                                HighSeverityWarning(
                                    f"Neat plugin failed to validate data model {data_model_file.name!r}: {e}"
                                )
                            )
                            continue
        return insights





from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    InsightList,
    ModelSyntaxWarning,
    Recommendation,
)

if TYPE_CHECKING:
    from cognite.neat._toolkit_adapter import NeatIssueList, SchemaLimits, SchemaSnapshot


class NeatPlugin:
    def __init__(self, client: ToolkitClient) -> None:
        from cognite.neat._toolkit_adapter import NeatClient

        self._client = NeatClient(client._config)
        self._cdf_snapshot: SchemaSnapshot | None = None
        self._cdf_limits: SchemaLimits | None = None

    def validate(self, data_model_dir: Path, data_model_file: Path) -> InsightList:
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
            cdf_snapshot=self.cdf_snapshot, limits=self.cdf_limits, modus_operandi="rebuild"
        )
        orchestrator.run(schema)

        return self.issues_to_insights(orchestrator.issues)

    @classmethod
    def issues_to_insights(cls, issues: NeatIssueList) -> InsightList:
        """Converts a list of Neat issues to a Toolkit insight list.

        Args:
            issues: List of Neat issues.

        Returns:
            InsightList: List of Toolkit insights.
        """
        from cognite.neat._toolkit_adapter import NeatConsistencyError, NeatModelSyntaxError, NeatRecommendation

        insights = InsightList()

        for issue in issues:
            if isinstance(issue, NeatModelSyntaxError):
                insights.append(ModelSyntaxWarning.model_validate(issue.model_dump()))
            elif isinstance(issue, NeatRecommendation):
                insights.append(Recommendation.model_validate(issue.model_dump()))
            elif isinstance(issue, NeatConsistencyError):
                insights.append(ConsistencyError.model_validate(issue.model_dump()))

        return insights

    @property
    def cdf_limits(self) -> SchemaLimits:
        from cognite.neat._toolkit_adapter import SchemaLimits

        if not self._cdf_limits:
            self._cdf_limits = SchemaLimits.from_api_response(self._client.statistics.project())
        return self._cdf_limits

    @property
    def cdf_snapshot(self) -> SchemaSnapshot:
        from cognite.neat._data_model._snapshot import SchemaSnapshot

        if not self._cdf_snapshot:
            self._cdf_snapshot = SchemaSnapshot.fetch_entire_cdf(self._client)
        return self._cdf_snapshot

    @classmethod
    def installed(cls) -> bool:
        """Check if neat is installed"""
        return find_spec("cognite.neat") is not None
