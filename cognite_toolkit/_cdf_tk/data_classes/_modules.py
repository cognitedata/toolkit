"""Compatibility wrapper for module loading data classes.

Historically, some parts of the code imported `ModuleRootDirectory` from
`cognite_toolkit._cdf_tk.data_classes._modules`. The canonical implementation
now lives in `cognite_toolkit._cdf_tk.data_classes.modules`.
"""

from cognite_toolkit._cdf_tk.data_classes.modules import Module, ModuleRootDirectory, Resource

__all__ = ["Module", "ModuleRootDirectory", "Resource"]
