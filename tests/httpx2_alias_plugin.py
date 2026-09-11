"""Alias ``httpx`` to ``httpx2`` before pytest loads the respx plugin.

respx type-checks mock responses as ``httpx.Response``. Toolkit talks HTTP through
``httpx2``, so without this alias those objects are distinct classes and mocks fail.
Must run before anything imports ``httpx`` or ``httpcore``.
"""

import httpx2

httpx2.alias_httpx()
