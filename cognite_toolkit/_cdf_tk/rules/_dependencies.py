from ._base import ToolkitGlobalRulSet


class Dependencies(ToolkitGlobalRulSet):
    def _dependency_validation(self, built_modules: list[BuiltModule], client: ToolkitClient | None) -> InsightList:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        built_resource_ids: set[tuple[type[ResourceCRUD], Identifier]] = {
            (resource.crud_cls, resource.identifier) for module in built_modules for resource in module.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceCRUD], dict[Identifier, list[BuiltResource]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for module in built_modules:
            for resource in module.resources:
                for crud_cls, dependency_id in resource.dependencies:
                    if (crud_cls, dependency_id) not in built_resource_ids:
                        missing_locally_by_crud_cls[crud_cls][dependency_id].append(resource)
        insights = InsightList()
        code = "MISSING-DEPENDENCY"
        if client:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                crud = crud_cls(client, None, None)
                display_name = crud.display_name
                existing_in_cdf = {
                    crud.get_id(cdf_item) for cdf_item in crud.retrieve(list(expected_by_identifier.keys()))
                }
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        expected_resources = expected_by_identifier[identifier]
                        referenced_str = " - ".join(
                            f"{resource.identifier!s} in {resource.source_path.as_posix()!r}"
                            for resource in expected_resources
                        )
                        insights.append(
                            ConsistencyError(
                                code=code,
                                message=f"{identifier} {display_name} does not exist locally or in CDF. It is referenced by: \n{referenced_str}",
                                fix=f"Ensure that {display_name} exists or removed the reference to it.",
                            )
                        )

        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    referenced_str = " - ".join(
                        f"{resource.identifier!s} in {resource.source_path.as_posix()!r}"
                        for resource in expected_resources
                    )
                    insights.append(
                        ConsistencyError(
                            code=code,
                            message=f"{identifier} {resource_type_name} does not exist. It is referenced by: \n{referenced_str}",
                            fix=f"If the {resource_type_name} exist in CDF, provide client credentials to not get this error. "
                            f"Or ensure that {resource_type_name} exists or removed the reference to it.",
                        )
                    )

        return insights
