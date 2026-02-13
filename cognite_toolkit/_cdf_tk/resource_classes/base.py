from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid"): ...


class ToolkitResource(BaseModelResource): ...
