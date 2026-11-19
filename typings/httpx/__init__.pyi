# Type-checking only: httpx2 is a drop-in replacement for httpx, and at runtime
# `httpx2.alias_httpx()` (see tests/httpx2_alias_plugin.py) makes `import httpx`
# resolve to httpx2. This stub mirrors that for ty, so third-party libraries
# typed against httpx (e.g. respx) accept httpx2 objects.
from httpx2 import *  # noqa: F403
from httpx2 import __all__ as __all__
