from pathlib import Path

import yaml


class FeatureFlag:
    _flags = None

    @staticmethod
    def _get_system_yaml_path() -> Path | None:
        loc = Path(__file__)
        while loc != loc.root:
            loc = loc.parent
            if (loc / "_system.yaml").exists():
                return loc / "_system.yaml"
        return None

    @staticmethod
    def enabled(flag: str) -> bool:
        if FeatureFlag._flags is None:
            system_yaml_path = FeatureFlag._get_system_yaml_path()
            if system_yaml_path:
                with open(system_yaml_path) as f:
                    FeatureFlag._flags = {k: v for k, v in yaml.safe_load(f).items() if k.startswith("FF_")}
            else:
                FeatureFlag._flags = {}

        return FeatureFlag._flags.get(flag, False)
