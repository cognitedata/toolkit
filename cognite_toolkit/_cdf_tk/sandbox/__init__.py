"""Sandbox session for incremental edits of Toolkit resources.

Loads the resources of modules with the same code as ``cdf build``, accepts generic path operations, checks
each change with the build's rules, and writes back only edited files with comments and variables preserved.
"""

from ._operations import AddResource, RemovePath, SetExtra, SetPath
from ._session import ApplyResult, ModelingSession

__all__ = [
    "AddResource",
    "ApplyResult",
    "ModelingSession",
    "RemovePath",
    "SetExtra",
    "SetPath",
]
