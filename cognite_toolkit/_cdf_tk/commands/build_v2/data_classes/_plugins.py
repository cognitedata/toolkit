from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    InsightList,
    ModelSyntaxError,
    Recommendation,
)

if TYPE_CHECKING:
    from cognite.neat._data_model._snapshot import SchemaSnapshot
    from cognite.neat._data_model.models.dms._limits import SchemaLimits
    from cognite.neat._issues import IssueList


class NeatPlugin:
    def __init__(self, client: ToolkitClient) -> None:

        from cognite.neat._client import NeatClient

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
        """

        from cognite.neat._data_model.importers import DMSAPIImporter
        from cognite.neat._data_model.rules.dms import DmsDataModelRulesOrchestrator

        importer = DMSAPIImporter.from_yaml(yaml_file=data_model_dir, data_model_file=data_model_file)
        schema = importer.to_data_model()

        orchestrator = DmsDataModelRulesOrchestrator(
            cdf_snapshot=self.cdf_snapshot,
            limits=self.cdf_limits,
        )
        orchestrator.run(schema)

        return self.issues_to_insights(orchestrator.issues)

    @classmethod
    def issues_to_insights(cls, issues: IssueList) -> InsightList:
        """Converts a list of Neat issues to a Toolkit insight list.

        Args:
            issues: List of Neat issues.

        Returns:
            InsightList: List of Toolkit insights.
        """
        from cognite.neat._issues import ConsistencyError as NeatConsistencyError
        from cognite.neat._issues import ModelSyntaxError as NeatModelSyntaxError
        from cognite.neat._issues import Recommendation as NeatRecommendation

        insights = InsightList()

        for issue in issues:
            if isinstance(issue, NeatModelSyntaxError):
                insights.append(ModelSyntaxError.model_validate(issue.model_dump()))
            elif isinstance(issue, NeatRecommendation):
                insights.append(Recommendation.model_validate(issue.model_dump()))
            elif isinstance(issue, NeatConsistencyError):
                insights.append(ConsistencyError.model_validate(issue.model_dump()))

        return insights

    @property
    def cdf_limits(self) -> SchemaLimits:
        from cognite.neat._data_model.models.dms._limits import SchemaLimits

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

        try:
            from cognite import neat

            _ = neat  # silence unused import

            return True
        except ImportError:
            return False
