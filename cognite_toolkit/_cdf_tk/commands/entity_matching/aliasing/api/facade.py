from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilderFactory,
    AliasingRule,
)


class AliasingFacade:
    def __init__(self, factory: AliasingKuiperBuilderFactory) -> None:
        self._factory = factory

    def generate(self, rules: list[AliasingRule]) -> AliasingKuiper:
        if not rules:
            raise ValueError("At least one rule must be provided")

        builder = self._factory.create()

        for rule in rules:
            builder.with_rule(rule)

        return builder.build()
