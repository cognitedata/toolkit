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
from __future__ import annotations

from pathlib import Path
from typing import final

from cognite_toolkit._cdf_tk.client import ToolkitClient

from .auth_loaders import GroupLoader
from .data_organization_loaders import DataSetsLoader
from .datamodel_loaders import SpaceLoader
from .extraction_pipeline_loaders import ExtractionPipelineLoader
from .timeseries_loaders import TimeSeriesLoader


@final
class GroupResourceScopedLoader(GroupLoader):
    dependencies = frozenset(
        {
            SpaceLoader,
            DataSetsLoader,
            ExtractionPipelineLoader,
            TimeSeriesLoader,
        }
    )

    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        super().__init__(client, build_dir, "resource_scoped_only")