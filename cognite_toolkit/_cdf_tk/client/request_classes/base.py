from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class BaseModelRequest(BaseModel):
    """Base class for all object. This includes resources and nested objects."""

    model_config = ConfigDict(alias_generator=to_camel, extra="ignore", populate_by_name=True)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the object to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.

        """
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)
