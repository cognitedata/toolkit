from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)


class DataModelReference(BaseModelObject):
    """Immutable reference to a data model."""

    space: str
    external_id: str
    version: str


class DataProductVersionBase(BaseModelObject):
    """Shared writable fields for data product versions.

    This is used as:
    - `initial_version` when creating/updating data products.
    - The base for `DataProductVersionResponse` when reading data products.
    """

    data_model: DataModelReference
    version_status: str | None = None  # draft, published, deprecated, archived
    usage_terms: str | None = None
    access: list[str] | None = None
    applications: dict[str, str] | None = None
    tags: dict[str, str] | None = None
    set_as_latest: bool = False


class DataProductVersionRequest(DataProductVersionBase, RequestResource):
    """Request resource for creating/updating data product versions."""

    def as_id(self) -> Identifier:
        """Return an identifier for this version.

        Data product versions are not exposed as a standalone CRUD resource in this
        PR, so this method is not expected to be called. It is implemented only to
        satisfy the abstract interface of ``RequestResource``.
        """
        raise NotImplementedError("DataProductVersionRequest.as_id is not supported.")


class DataProductVersionResponse(DataProductVersionBase, ResponseResource[DataProductVersionRequest]):
    """Response resource for data product versions embedded on data products.

    A dedicated CRUD/API for data product versions is intentionally not exposed in this PR;
    this model only represents the nested version information on data products.
    """

    version_id: int
    is_latest: bool = False
    created_time: int | None = None
    last_updated_time: int | None = None

    def as_request_resource(self) -> DataProductVersionRequest:
        """Return a request representation of this version.

        Note:
            Data product versions are currently only exposed as nested data
            on data products. This method exists primarily to satisfy the
            ``ResponseResource`` interface and is not used by the Toolkit's
            CRUD layer in this PR.
        """
        return DataProductVersionRequest(
            data_model=self.data_model,
            version_status=self.version_status,
            usage_terms=self.usage_terms,
            access=self.access,
            applications=self.applications,
            tags=self.tags,
            set_as_latest=self.set_as_latest,
        )
