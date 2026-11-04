from pathlib import Path
from cognite_toolkit._cdf_tk.commands._changes import RemoveOutdatedAlphaFlags

class TestRemoveOutdatedFlags:
     def test_remove_outdated_flags(self, tmp_path: Path):
         cdf_toml = """[cdf]
default_organization_dir = "cognite_toolkit"

[alpha_flags]
alpha-rules = true # My comment
outdated-flag = true
another-outdated-flag = false

[plugins]
data = true

[modules]
# This is the version of the modules. It should not be changed manually.
# It will be updated by the 'cdf modules upgrade' command.
version = "0.0.0"
"""

         cdf_toml_path = tmp_path / "cdf.toml"
         cdf_toml_path.write_text(cdf_toml, encoding="utf-8")
         changed = RemoveOutdatedAlphaFlags(tmp_path, None).do()
         assert changed == {cdf_toml_path}


         assert cdf_toml_path.read_text() == """[cdf]
default_organization_dir = "cognite_toolkit"

[alpha_flags]
alpha-rules = true # My comment

[plugins]
data = true

[modules]
# This is the version of the modules. It should not be changed manually.
# It will be updated by the 'cdf modules upgrade' command.
version = "0.0.0"
"""
