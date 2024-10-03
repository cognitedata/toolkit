from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FunctionScheduleID:
    function_external_id: str
    name: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "functionExternalId" if camel_case else "function_external_id": self.function_external_id,
            "name": self.name,
        }
