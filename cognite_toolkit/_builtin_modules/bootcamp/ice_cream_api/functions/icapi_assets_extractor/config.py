from dataclasses import dataclass

from cognite.extractorutils.configtools import BaseConfig


@dataclass
class DestConfig:
    database: str
    table: str

@dataclass
class ExtractorConfig:
    api_url: str
    dest: DestConfig

@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
