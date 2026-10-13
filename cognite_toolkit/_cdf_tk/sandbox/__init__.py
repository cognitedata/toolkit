"""Sandbox session for incremental data modeling edits.

Loads Space, Container, View and DataModel YAML into the same *Request
classes used by deploy, accepts typed operations, and writes back only
dirty files via ResourceReplacer + YAMLComments.
"""

from ._operations import (
    AddContainer,
    AddContainerProperty,
    AddView,
    AddViewProperty,
    AddViewToDataModel,
    RemoveContainerProperty,
    RemoveViewProperty,
    SetTransformationQuery,
)
from ._session import ApplyResult, ModelingSession

__all__ = [
    "AddContainer",
    "AddContainerProperty",
    "AddView",
    "AddViewProperty",
    "AddViewToDataModel",
    "ApplyResult",
    "ModelingSession",
    "RemoveContainerProperty",
    "RemoveViewProperty",
    "SetTransformationQuery",
]
