from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

FLAGS = {
    "interactive_init": {
        "visible": True,
        "description": "Enables interactive init",
    },
    "print_flags": {
        "visible": True,
        "description": "Does nothing",
    },
}


class FeatureFlag:
    @staticmethod
    def _get_file() -> Path:
        f = Path(tempfile.gettempdir()) / "tk-ff.bin"
        if not f.exists():
            f.write_text("{}")
        return f

    @staticmethod
    def _load_user_settings() -> dict[str, bool]:
        f = FeatureFlag._get_file()
        return yaml.safe_load(f.read_text())

    @staticmethod
    def _save_user_settings(flags: dict[str, bool]) -> None:
        f = FeatureFlag._get_file()
        settings = FeatureFlag._load_user_settings()
        settings.update(flags)
        f.write_text(yaml.dump(settings))

    @staticmethod
    def list() -> list[tuple[str, str, bool]]:
        user_settings = FeatureFlag._load_user_settings()
        flags = []
        for k, v in FLAGS.items():
            is_enabled = user_settings.get(k, False)
            if is_enabled or v.get("visible", False):
                flags.append((k, str(v.get("description", "")), is_enabled))
        return flags

    @staticmethod
    def set(flag: str, value: bool) -> None:
        if flag not in FLAGS:
            raise ValueError(f"Unknown flag: {flag}")

    @staticmethod
    def is_enabled(flag: str) -> bool:
        return False
