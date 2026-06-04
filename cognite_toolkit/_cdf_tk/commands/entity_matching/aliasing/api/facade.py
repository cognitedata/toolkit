from collections.abc import Callable

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilder,
    AliasingRule,
)


class AliasingFacade:
    def __init__(self, builder_provider: Callable[[], AliasingKuiperBuilder]) -> None:
        self._builder_provider = builder_provider

    def generate(self, rules: list[AliasingRule]) -> AliasingKuiper:
        if not rules:
            raise ValueError("At least one rule must be provided")

        builder = self._builder_provider()

        for rule in rules:
            builder.with_rule(rule)

        return builder.build()
