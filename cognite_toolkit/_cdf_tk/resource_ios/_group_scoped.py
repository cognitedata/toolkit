# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from pathlib import Path
from typing import final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient

from ._auth import GroupIO, SecurityCategoryIO
from ._classic import AssetIO
from ._data_organization import DataSetsIO
from ._data_product import DataProductIO
from ._datamodel import SpaceCRUD
from ._extraction_pipeline import ExtractionPipelineIO
from ._location import LocationFilterIO
from ._raw import RawDatabaseCRUD, RawTableCRUD
from ._timeseries import TimeSeriesCRUD


@final
class GroupResourceScopedCRUD(GroupIO):
    dependencies = frozenset(
        {
            SpaceCRUD,
            DataSetsIO,
            DataProductIO,
            ExtractionPipelineIO,
            TimeSeriesCRUD,
            SecurityCategoryIO,
            LocationFilterIO,
            AssetIO,
            RawDatabaseCRUD,
            RawTableCRUD,
        }
    )

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None):
        super().__init__(client, build_dir, console, "resource_scoped_only")

    @property
    def display_name(self) -> str:
        return "resource-scoped groups"
