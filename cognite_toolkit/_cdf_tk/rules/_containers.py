from collections.abc import Iterable

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    ConsistencyError,
    Insight,
    v09_gate,
)
from cognite_toolkit._cdf_tk.rules._base import ToolkitLocalRule, with_position
from cognite_toolkit._cdf_tk.yaml_classes import ContainerYAML


class DeployableContainer(ToolkitLocalRule):
    """
    Checks that the container can be deployed

    ## What it does
    This rule checks that the container can be deployed

    ## Why is this bad?
    An invalid container will be rejected by the CDF API, and the deployment will fail.

    ## Example
    **Bad**: Container with non-nullable direct relations
    ```yaml
    space: sp_schema
    externalId: FailingContainer
    usedFor: node
    properties:
      faultyPointer:
        nullable: false # This is not allowed for direct relations
        type:
          type: direct
          list: false
          container:
              type: container
              space: cdf_cdm
              externalId: CogniteDescribable
    ```

    **Good**: Container with nullable direct relations
    ```yaml
    space: sp_schema
    externalId: FailingContainer
    usedFor: node
    properties:
      faultyPointer:
        nullable: true
        type:
          type: direct
          list: false
          container:
              type: container
              space: cdf_cdm
              externalId: CogniteDescribable
    ```
    """

    CODE = "INVALID-DIRECT-RELATION"
    LEGACY_CODE = "DMS-CONTAINER-001"  # Used when the v09 flag is not enabled
    insight_type = ConsistencyError

    def validate(self) -> Iterable[Insight]:
        for resource, source_file in self._get_validated_resources_with_file():
            if not isinstance(resource, ContainerYAML):
                continue
            invalid_direct_relations: list[str] = []
            for prop_name, prop in resource.properties.items():
                if prop.type.type == "direct" and prop.nullable is False:
                    invalid_direct_relations.append(prop_name)
            if invalid_direct_relations:
                message = f"Container {resource.as_id()!s} has non-nullable direct relations: {', '.join(invalid_direct_relations)}"
                fix = (
                    f"Make the following properties nullable: {', '.join(invalid_direct_relations)}. "
                    "Direct relations must be nullable."
                )
                yield with_position(
                    v09_gate(BuildError, ConsistencyError)(
                        message=message,
                        code=v09_gate(self.CODE, self.LEGACY_CODE),
                        fix=fix,
                        source_file=source_file.source_path,
                        alpha=True,
                    ),
                    keys=invalid_direct_relations if len(invalid_direct_relations) == 1 else (),
                )
