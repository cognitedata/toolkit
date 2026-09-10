from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from ._constants import VALID_LOGIN_FLOWS, VALID_PROVIDERS
from ._types import LoginFlow, Provider


@dataclass
class EnvOptions(Mapping):
    display_name: str
    default_example: str = ""
    example: dict[Provider, str] = field(default_factory=dict)
    is_secret: bool = False
    required: frozenset[tuple[Provider | None, LoginFlow]] = frozenset()
    optional: frozenset[tuple[Provider | None, LoginFlow]] = frozenset()

    def __getitem__(self, key: str) -> str | bool:
        return self.__dict__[key]

    def __iter__(self) -> Iterable[str]:  # type: ignore[override]
        return iter(self.__dict__.keys())

    def __len__(self) -> int:
        return len(self.__dict__)


ALL_CASES = [(None, flow) for flow in VALID_LOGIN_FLOWS]


def all_providers(
    flow: LoginFlow, exclude: Provider | Iterable[Provider] | None = None
) -> frozenset[tuple[Provider | None, LoginFlow]]:
    if isinstance(exclude, str):
        exclude = {exclude}
    elif exclude is not None:
        exclude = set(exclude)
    return frozenset((prov, flow) for prov in VALID_PROVIDERS if exclude is None or prov not in exclude)
