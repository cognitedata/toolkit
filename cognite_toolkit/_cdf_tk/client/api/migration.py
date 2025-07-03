from collections.abc import Sequence
from itertools import groupby

from cognite.client.data_classes.data_modeling import NodeList, filters, query

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, Mapping
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .extended_data_modeling import ExtendedInstancesAPI


class MappingAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = Mapping.get_source()

    def retrieve(self, ids: Sequence[AssetCentricId]) -> NodeList[Mapping]:
        """Retrieve a list of mappings by their IDs.

        Args:
            ids (Sequence[AssetCentricId]): A sequence of AssetCentricId objects representing the IDs of the mappings to retrieve.
        """
        results: NodeList[Mapping] = NodeList[Mapping]([])
        for chunk in chunker_sequence(ids, self._RETRIEVE_LIMIT):
            retrieve_query = query.Query(
                with_={
                    "mappings": query.NodeResultSetExpression(
                        filter=filters.And(filters.HasData(views=[self._view_id]), self._create_dms_filter(ids)),
                        limit=len(chunk),
                    ),
                },
                select={"mappings": query.Select([query.SourceSelector(self._view_id, ["*"])])},
            )
            chunk_response = self._instance_api.query(retrieve_query)
            results.extend([Mapping._load(item.dump()) for item in chunk_response.get("mappings", [])])
        return results

    @staticmethod
    def _create_dms_filter(ids: Sequence[AssetCentricId]) -> filters.Filter:
        """Create a filter that matches all the AssetCentricIds in the list."""
        if not ids:
            raise ValueError("Cannot create a filter from an empty AssetCentricIdList.")
        to_or_filters: list[filters.Filter] = []
        mapping_view = Mapping.get_source()
        for resource_type, resource_ids in groupby(
            sorted(ids, key=lambda x: x.resource_type), key=lambda x: x.resource_type
        ):
            is_resource = filters.Equals(mapping_view.as_property_ref("resourceType"), resource_type)
            is_id = filters.In(mapping_view.as_property_ref("id"), [resource_id.id_ for resource_id in resource_ids])
            to_or_filters.append(filters.And(is_resource, is_id))
        return filters.Or(*to_or_filters)


class MigrationAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self.mapping = MappingAPI(instance_api)
