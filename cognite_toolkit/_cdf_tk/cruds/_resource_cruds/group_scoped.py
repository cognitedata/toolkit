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

from .auth import GroupCRUD, SecurityCategoryCRUD
from .classic import AssetCRUD
from .data_organization import DataSetsCRUD
from .datamodel import SpaceCRUD
from .extraction_pipeline import ExtractionPipelineCRUD
from .location import LocationFilterCRUD
from .raw import RawDatabaseCRUD, RawTableCRUD
from .timeseries import TimeSeriesCRUD


@final
class GroupResourceScopedCRUD(GroupCRUD):
    dependencies = frozenset(
        {
            SpaceCRUD,
            DataSetsCRUD,
            ExtractionPipelineCRUD,
            TimeSeriesCRUD,
            SecurityCategoryCRUD,
            LocationFilterCRUD,
            AssetCRUD,
            RawDatabaseCRUD,
            RawTableCRUD,
        }
    )

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None):
        super().__init__(client, build_dir, console, "resource_scoped_only")

    @property
    def display_name(self) -> str:
        return "resource-scoped groups"
