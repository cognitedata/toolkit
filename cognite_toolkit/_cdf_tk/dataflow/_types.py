from enum import Enum

from cognite_toolkit._cdf_tk.utils.sql_sources import Confidence, ParseStatus


class FlowKind(str, Enum):
    """How the subject of an edge relates to the object. An edge reads "subject verb object"."""

    READS = "reads"
    WRITES = "writes"
    TRIGGERS = "triggers"
    CONTAINS = "contains"
    REQUIRES = "requires"


__all__ = ["Confidence", "FlowKind", "ParseStatus"]
