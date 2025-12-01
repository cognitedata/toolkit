from __future__ import annotations

import pytest

from cognite_toolkit import _cdf
from cognite_toolkit._cdf_tk import cdf_toml
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag


@pytest.fixture
def reset_cdf_toml_singleton():
    """Reset CDFToml singleton before and after each test to ensure test isolation.

    Use this fixture in tests that need to load cdf.toml from a test directory
    to avoid conflicts with the repo's cdf.toml singleton.
    """
    cdf_toml._CDF_TOML = None
    _cdf.CDF_TOML = None  # Also reset the module-level instance in _cdf.py
    FeatureFlag.flush()  # Also clear the feature flag cache
    yield
    cdf_toml._CDF_TOML = None
    _cdf.CDF_TOML = None
    FeatureFlag.flush()
