from collections.abc import Sequence

from pydantic import BaseModel

from ._insights import Insight
from ._module_results import ModuleResult


class BuiltResult(BaseModel):
    module_results: Sequence[ModuleResult]
    global_insights: Sequence[Insight]
