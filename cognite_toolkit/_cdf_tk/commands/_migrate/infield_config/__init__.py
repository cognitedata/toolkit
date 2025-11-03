"""InField V2 configuration migration module.

This module handles migration from old APM Config format to InField V2 configuration format.
The migration is split into separate modules for better organization and maintainability.
"""

from .migration import create_infield_v2_config

__all__ = ["create_infield_v2_config"]

