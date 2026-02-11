from collections.abc import Sequence

from pydantic import BaseModel

from ._insights import Insight
from ._module_results import ModuleResult


class BuiltResult(BaseModel):
    module_results: Sequence[ModuleResult]
    global_insights: Sequence[Insight]

    @property
    def insights(self) -> Sequence[Insight]:
        """Returns all insights from the module results and global insights."""
        all_insights: list[Insight] = []
        for module_result in self.module_results:
            all_insights.extend(module_result.insights)
        all_insights.extend(self.global_insights)
        return all_insights
