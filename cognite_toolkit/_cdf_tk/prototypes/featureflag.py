from __future__ import annotations


class FeatureFlag:
    @staticmethod
    def enabled(flag: str) -> bool:
        return False
