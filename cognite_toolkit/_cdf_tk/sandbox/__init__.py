"""Sandbox session for incremental edits of Toolkit resources.

Loads the resources of modules with the same code as ``cdf build``, accepts generic path operations, checks
each change with the build's rules, and writes back only edited files with comments and variables preserved.
Can also plan and commit against CDF when a ToolkitClient is provided.

The package does not import its submodules here. ``python -m cognite_toolkit._cdf_tk.sandbox`` has to enable
the alpha flags before the resource classes are imported, and importing the package runs this file first.
"""
