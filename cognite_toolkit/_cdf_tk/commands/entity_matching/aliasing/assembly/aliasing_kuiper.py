from dataclasses import dataclass


@dataclass(frozen=True)
class AliasingKuiper:
    expression: str
