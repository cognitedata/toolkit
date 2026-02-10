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
