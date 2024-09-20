from dataclasses import dataclass

from cognite.extractorutils.configtools import BaseConfig


@dataclass
class ExtractorConfig:
    api_url: str
    data_set_ext_id: str

@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
