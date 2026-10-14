"""Sandbox session for incremental edits of Toolkit resources.

Loads the resources of modules with the same code as ``cdf build``, accepts generic path operations, checks
each change with the build's rules, and writes back only edited files with comments and variables preserved.
Can also plan and commit against CDF when a ToolkitClient is provided.
"""

from ._operations import AddResource, RemovePath, RemoveResource, SetExtra, SetPath
from ._session import ApplyResult, ModelingSession
from ._store import CdfScope, CdfStore, YamlStore

__all__ = [
    "AddResource",
    "ApplyResult",
    "CdfScope",
    "CdfStore",
    "ModelingSession",
    "RemovePath",
    "RemoveResource",
    "SetExtra",
    "SetPath",
    "YamlStore",
]
