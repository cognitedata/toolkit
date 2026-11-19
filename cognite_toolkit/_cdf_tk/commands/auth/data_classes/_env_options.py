from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass, field

from ._constants import LOGIN_FLOWS, PROVIDERS
from ._types import LoginFlow, Provider


@dataclass
class EnvOptions(Mapping[str, str | bool]):
    display_name: str
    default_example: str = ""
    example: dict[Provider, str] = field(default_factory=dict)
    is_secret: bool = False
    required: frozenset[tuple[Provider | None, LoginFlow]] = frozenset()
    optional: frozenset[tuple[Provider | None, LoginFlow]] = frozenset()

    def __getitem__(self, key: str) -> str | bool:
        return self.__dict__[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__.keys())

    def __len__(self) -> int:
        return len(self.__dict__)


ALL_CASES = [(None, flow) for flow in LOGIN_FLOWS]


def all_providers(
    flow: LoginFlow, exclude: Provider | Collection[Provider] | None = None
) -> frozenset[tuple[Provider | None, LoginFlow]]:
    excluded: set[str] = set()
    if isinstance(exclude, str):
        excluded.add(exclude)
    elif exclude is not None:
        excluded.update(exclude)
    return frozenset((prov, flow) for prov in PROVIDERS if prov not in excluded)
