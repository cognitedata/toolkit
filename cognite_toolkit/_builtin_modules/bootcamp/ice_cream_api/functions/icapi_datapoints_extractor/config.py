from dataclasses import dataclass, field
from typing import List

from cognite.extractorutils.configtools import BaseConfig, RawStateStoreConfig, StateStoreConfig


@dataclass
class ExtractorConfig:
    api_url: str
    backfill: bool
    data_set_ext_id: str
    hours: int
    sites: List[str]
    state_store: StateStoreConfig = field(
        default_factory=StateStoreConfig(raw=RawStateStoreConfig(database=None, table=None))
    )


@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
