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

import json
import os
import logging
from typing import List
from cognite.client.exceptions import CogniteAuthError
from cognite.client.data_classes.data_sets import DataSet
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials, Token

logger = logging.getLogger(__name__)


# To add a new example, add a new entry here with the same name as the folder
# These values are used by the python scripts.
class CDFToolConfig:
    """Configurations for how to store data in CDF

    Properties:
        client: active CogniteClient
        example: name of the example folder you want to use
    Functions:
        config: configuration for the example (.get("config_name"))
        verify_client: verify that the client has correct credentials and specified access capabilties
        veryify_dataset: verify that the data set exists and that the client has access to it

    To add a new example, add a new entry here with the same name as the folder.
    These values are used by the python scripts.
    """

    def __init__(
        self,
        client_name: str = "Generic Cognite examples library",
        config: dict | None = None,
        token: str = None,
    ) -> None:
        self._data_set_id: int = 0
        self._example = None
        self._failed = False
        self._environ = {}
        if token is not None:
            self._environ["CDF_TOKEN"] = token
        if config is not None:
            self._config = config
        else:
            try:
                with open(f"./inventory.json", "rt") as file:
                    self._config = json.load(file)
            except Exception as e:
                logger.info(
                    "Not loading configuration from inventory.json file, using 'default' as values for all attributes."
                )
                self._config = {
                    "default": {
                        "raw_db": "default",
                        "data_set": "default",
                        "data_set_desc": "-",
                        "model_space": "default",
                        "data_model": "default",
                    }
                }
                self._example = "default"
        if (
            self.environ("CDF_URL", default=None, fail=False) is None
            and self.environ("CDF_CLUSTER", default=None, fail=False) is None
        ):
            # If CDF_URL and CDF_CLUSTER are not set, we may be in a Jupyter notebook in Fusion,
            # and credentials are preset to logged in user (no env vars are set!).
            self._client = CogniteClient()
            return

        # CDF_CLUSTER and CDF_PROJECT are minimum requirements to know where to connect.
        # Above they were forced default to None and fail was False, here we
        # will fail with an exception if they are not set.
        self._cluster = self.environ("CDF_CLUSTER")
        self._project = self.environ("CDF_PROJECT")
        # CDF_URL is optional, but if set, we use that instead of the default URL using cluster.
        self._cdf_url = self.environ(
            "CDF_URL", f"https://{self._cluster}.cognitedata.com"
        )
        # If CDF_TOKEN is set, we want to use that token instead of client credentials.
        if (
            self.environ("CDF_TOKEN", default=None, fail=False) is not None
            or token is not None
        ):
            self._client = CogniteClient(
                ClientConfig(
                    client_name=client_name,
                    base_url=self._cdf_url,
                    project=self._project,
                    credentials=Token(token or self.environ("CDF_TOKEN")),
                )
            )
        else:
            # We are now doing OAuth2 client credentials flow, so we need to set the
            # required variables.
            # We can infer scopes and audience from the cluster value.
            # However, the URL to use to retrieve the token, as well as
            # the client id and secret, must be set as environment variables.
            self._scopes = self.environ(
                "IDP_SCOPES",
                [f"https://{self._cluster}.cognitedata.com/.default"],
            )
            self._audience = self.environ(
                "IDP_AUDIENCE", f"https://{self._cluster}.cognitedata.com"
            )
            self._client = CogniteClient(
                ClientConfig(
                    client_name=client_name,
                    base_url=self._cdf_url,
                    project=self._project,
                    credentials=OAuthClientCredentials(
                        token_url=self.environ("IDP_TOKEN_URL"),
                        client_id=self.environ("IDP_CLIENT_ID"),
                        # client secret should not be stored in-code, so we load it from an environment variable
                        client_secret=self.environ("IDP_CLIENT_SECRET"),
                        scopes=self._scopes,
                        audience=self._audience,
                    ),
                )
            )

    def __str__(self):
        return (
            f"Cluster {self._cluster} with project {self._project} and config:\n"
            + json.dumps(self._environ, indent=2, sort_keys=True)
        )

    @property
    # Flag set if something that should have worked failed if a data set is
    # loaded and/or deleted.
    def failed(self) -> bool:
        return self._failed

    @failed.setter
    def failed(self, value: bool):
        self._failed = value

    @property
    def client(self) -> CogniteClient:
        return self._client

    @property
    def data_set_id(self) -> int:
        return self._data_set_id

    # Use this to ignore the data set when verifying the client's access capabilities
    # Set the example property to configure the data set and verify it
    def clear_dataset(self):
        self._data_set_id = 0

    def config(self, attr: str) -> str:
        """Helper function to get configuration for an example (from inventory.json).

        This function uses the example property in this class as a key to get the configuration,
        so example must be set before calling the function.

        Args:
            attr: name of configuration variable

        Yields:
            Value of the configuration variable for example
            Raises ValueError if configuration variable is not set
        """
        if attr not in self._config.get(self._example, {}):
            raise ValueError(
                f"{attr} property must be set in CDFToolConfig()/inventory.json."
            )
        return self._config.get(self._example, {}).get(attr, "")

    def environ(
        self, attr: str, default: str | List[str] = None, fail: bool = True
    ) -> str:
        """Helper function to load variables from the environment.

        Use python-dotenv to load environment variables from an .env file before
        using this function.

        If the environment variable has spaces, it will be split into a list of strings.

        Args:
            attr: name of environment variable
            default: default value if environment variable is not set
            fail: if True, raise ValueError if environment variable is not set

        Yields:
            Value of the environment variable
            Raises ValueError if environment variable is not set and fail=True
        """
        if attr in self._environ and self._environ[attr] is not None:
            return self._environ[attr]
        # If the var was none, we want to re-evaluate from environment.
        self._environ[attr] = os.environ.get(attr, None)
        if self._environ[attr] is None:
            if default is None and fail:
                raise ValueError(
                    f"{attr} property is not available as an environment variable and no default set."
                )
            self._environ[attr] = default
        return self._environ[attr]

    @property
    def example(self) -> str:
        return self._example

    @example.setter
    def example(self, value: str):
        if value is None or value not in self._config:
            raise ValueError(
                "example must be set to one of the values in the inventory.json file used by CDFToolConfig()."
            )
        self._example = value
        # Since we now have a new configuration, check the dataset and set the id
        self._data_set_id = self.verify_dataset()

    def verify_client(self, capabilities: dict[list] | None = None) -> CogniteClient:
        """Verify that the client has correct credentials and required access rights

        Supply requirement CDF ACLs to verify if you have correct access
        capabilities = {
            "filesAcl": ["READ", "WRITE"],
            "datasetsAcl": ["READ", "WRITE"]
        }
        The data_set_id will be used when verifying that the client has access to the dataset.
        This approach can be reused for any usage of the Cognite Python SDK.

        Args:
            capabilities (dict[list], optional): access capabilities to verify
            data_set_id (int): id of dataset that access should be granted to

        Yields:
            CogniteClient: Verified client with access rights
            Re-raises underlying SDK exception
        """
        capabilities = capabilities or {}
        try:
            # Using the token/inspect endpoint to check if the client has access to the project.
            # The response also includes access rights, which can be used to check if the client has the
            # correct access for what you want to do.
            resp = self.client.iam.token.inspect()
            if resp is None or len(resp.capabilities) == 0:
                raise CogniteAuthError(
                    f"Don't have any access rights. Check credentials."
                )
        except Exception as e:
            raise e
        # iterate over all the capabilties we need
        for cap, actions in capabilities.items():
            # Find the right capability in our granted capabilities
            for k in resp.capabilities:
                if len(k.get(cap, {})) == 0:
                    continue
                # For each of the actions (e.g. READ or WRITE) we need, check if we have it
                for a in actions:
                    if a not in k.get(cap, {}).get("actions", []):
                        raise CogniteAuthError(
                            f"Don't have correct access rights. Need {a} on {cap}"
                        )
                # Check if we either have all scope or data_set_id scope
                if "all" not in k.get(cap, {}).get("scope", {}) and (
                    self._data_set_id != 0
                    and str(self._data_set_id)
                    not in k.get(cap, {})
                    .get("scope", {})
                    .get("datasetScope")
                    .get("ids", [])
                ):
                    raise CogniteAuthError(
                        f"Don't have correct access rights. Need {a} on {cap}"
                    )
                continue
        return self._client

    def verify_dataset(self, data_set_name: str = None) -> int | None:
        """Verify that the configured data set exists and is accessible

        This function can be used independent of example config by supplying the data set name.
        It will then ignore the example config and use the supplied name.
        Calling this function directly will not influence verify_client().

        Args:
            data_set_name (str, optional): name of the data set to verify
        Yields:
            data_set_id (int)
            Re-raises underlying SDK exception
        """

        def get_dataset_name() -> str:
            """Helper function to get the dataset name from the inventory.json file"""
            return (
                data_set_name
                if data_set_name is not None and len(data_set_name) > 0
                else self.config("data_set")
            )

        client = self.verify_client(capabilities={"datasetsAcl": ["READ", "WRITE"]})
        try:
            data_set = client.data_sets.retrieve(external_id=get_dataset_name())
            if data_set is not None:
                return data_set.id
        except Exception as e:
            raise CogniteAuthError(
                f"Don't have correct access rights. Need READ and WRITE on datasetsAcl."
            )
        try:
            # name can be empty, but is useful for UI purposes
            data_set = DataSet(
                external_id=get_dataset_name(),
                name=get_dataset_name(),
                description=self.config("data_set_desc")
                if self.config("data_set_desc") != ""
                else "Test data set for tutorials",
            )
            data_set = client.data_sets.create(data_set)
            return data_set.id
        except Exception as e:
            raise CogniteAuthError(
                f"Don't have correct access rights. Need also WRITE on "
                + "datasetsAcl or that the data set {get_dataset_name()} has been created."
            )
