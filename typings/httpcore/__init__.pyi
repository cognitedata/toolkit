# Type-checking only: mirrors `httpx2.alias_httpx()`, which aliases httpcore to httpcore2 at runtime.
from httpcore2 import *  # noqa: F403
from httpcore2 import __all__ as __all__
