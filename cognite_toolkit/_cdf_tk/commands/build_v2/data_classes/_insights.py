import csv
import io
from collections import UserList, defaultdict

from pydantic import BaseModel


class Insight(BaseModel):
    """Base class for all insights"""

    message: str
    code: str | None = None
    fix: str | None = None

    @classmethod
    def insight_type(cls) -> str:
        return cls.__name__


class ModelSyntaxError(Insight):
    """If any syntax error is found. Stop validation
    and ask user to fix the syntax error first."""

    ...


class ConsistencyError(Insight):
    """If any consistency error is found, the deployment of the CDF resource will fail."""

    ...


class ConsistencyWarning(Insight):
    """Typically handles validations with extras=True, where internal representation might be off sync with
    CDF API definition.
    """

    ...


class Recommendation(Insight):
    """Best practice recommendation."""

    ...


class InsightList(UserList[Insight]):
    """A list of insights that can be sorted by type and message."""

    def by_type(self) -> dict[type[Insight], list[Insight]]:
        """Returns a dictionary of insights sorted by their type."""
        result: dict[type[Insight], list[Insight]] = defaultdict(list)
        for insight in self.data:
            insight_type = type(insight)
            if insight_type not in result:
                result[insight_type] = []
            result[insight_type].append(insight)
        return result

    def by_code(self) -> dict[str, list[Insight]]:
        """Returns a dictionary of insights sorted by their code."""
        result: dict[str, list[Insight]] = defaultdict(list)
        for insight in self.data:
            if insight.code is not None:
                result[insight.code].append(insight)
            else:
                result["UNDEFINED"].append(insight)
        return dict(result)

    @property
    def has_model_syntax_errors(self) -> bool:
        """Returns True if there are any model syntax errors in the insights."""
        return any(isinstance(insight, ModelSyntaxError) for insight in self.data)

    @property
    def summary(self) -> dict[str, int]:
        """Returns a summary dict with breakdown of insights by type.

        Returns:
            Dict with keys: syntax_errors, consistency_errors, recommendations
        """
        breakdown = {
            "syntax_errors": 0,
            "consistency_errors": 0,
            "recommendations": 0,
        }

        for insight in self.data:
            if isinstance(insight, ModelSyntaxError):
                breakdown["syntax_errors"] += 1
            elif isinstance(insight, ConsistencyError):
                breakdown["consistency_errors"] += 1
            elif isinstance(insight, Recommendation):
                breakdown["recommendations"] += 1

        return breakdown

    def to_csv(self) -> str:
        """Returns a CSV formatted string representation of the insights.

        Returns:
            CSV formatted string with columns: insight_type, code, message, fix
        """
        output = io.StringIO()
        fieldnames = ["insight_type", "code", "message", "fix"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for insight in self.data:
            writer.writerow(
                {
                    "insight_type": insight.insight_type(),
                    "code": insight.code or "",
                    "message": insight.message,
                    "fix": insight.fix or "",
                }
            )

        return output.getvalue()
