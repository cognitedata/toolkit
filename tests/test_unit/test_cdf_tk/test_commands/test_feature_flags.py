from cognite_toolkit._cdf_tk.commands.featureflag import FeatureFlag


class TestFeatureCommand:
    def test_unknown_flag_returns_false(self):
        assert FeatureFlag.is_enabled("unknown_flag") is False
