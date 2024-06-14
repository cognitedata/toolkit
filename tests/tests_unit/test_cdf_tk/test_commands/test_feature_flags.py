from cognite_toolkit._cdf_tk.commands.featureflag import FeatureFlag, Flags


class TestFeatureCommand:
    def test_unknown_flag_returns_false(self):
        assert FeatureFlag.is_enabled("unknown_flag") is False

    def test_user_setting_is_stored(self):
        FeatureFlag.reset_user_settings()
        assert FeatureFlag.is_enabled("INTERACTIVE_INIT") is False

        FeatureFlag.save_user_settings(FeatureFlag.to_flag("interactive_init"), True)
        assert FeatureFlag.is_enabled(Flags.INTERACTIVE_INIT)
