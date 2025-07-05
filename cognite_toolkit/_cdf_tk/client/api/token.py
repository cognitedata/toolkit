from functools import cached_property

from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import _CAPABILITY_CLASS_BY_NAME, AllScope, Capability
from cognite.client.data_classes.iam import TokenInspection

from cognite_toolkit._cdf_tk.client.data_classes.capabilities import scope_intersection, scope_union
from cognite_toolkit._cdf_tk.utils import humanize_collection

_ACL_CLASS_BY_CLASS_NAME = {cap.__name__: cap for cap in _CAPABILITY_CLASS_BY_NAME.values()}


class TokenAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    @cached_property
    def token(self) -> TokenInspection:
        return self._client.iam.token.inspect()

    def get_scope(self, actions: list[Capability.Action]) -> list[Capability.Scope] | None:
        """Gets the scopes for the given actions and ACL class from the token.

        Args:
            actions:  List of actions to get scopes for.

        Returns:
            A list of scopes that apply to all the actions, or None if no scopes are found.
        """
        acl_cls = self._get_acl_class(actions)
        # First, we find the union of all scopes for a given action,
        # then we find the intersection of those scopes across all actions.
        scope_union_by_action_by_scope_cls = self._union_by_action_by_scope_cls(acl_cls, set(actions))
        if not scope_union_by_action_by_scope_cls:
            return None
        return self._intersection_over_actions(scope_union_by_action_by_scope_cls)

    @staticmethod
    def _get_acl_class(actions: list[Capability.Action]) -> type[Capability]:
        """Returns the ACL class that matches the given actions."""
        classes: set[type[Capability]] = set()
        for action in actions:
            action_class_name = type(action).__qualname__.split(".")[0]
            if action_class_name not in _ACL_CLASS_BY_CLASS_NAME:
                raise ValueError(f"Unknown action class: {action_class_name}")
            classes.add(_ACL_CLASS_BY_CLASS_NAME[action_class_name])
        if len(classes) > 1:
            raise ValueError(
                f"Actions belong to multiple ACL classes: {humanize_collection([cls.__name__ for cls in classes])}."
            )
        if len(classes) == 0:
            raise ValueError("No actions provided to get_scope")
        return next(iter(classes))

    @staticmethod
    def _intersection_over_actions(
        scopes_by_action: dict[Capability.Action, dict[type[Capability.Scope], Capability.Scope]],
    ) -> list[Capability.Scope] | None:
        all_scope_actions = {
            action for action, scopes_by_cls in scopes_by_action.items() if AllScope in scopes_by_cls.keys()
        }
        if len(all_scope_actions) == len(scopes_by_action):
            # Special case: if AllScope is in all actions, return it directly
            return [AllScope()]
        intersection_scopes = set.intersection(
            *[
                set(scopes_by_cls.keys())
                for action, scopes_by_cls in scopes_by_action.items()
                if action not in all_scope_actions
            ]
        )
        if not intersection_scopes:
            return None
        output_scopes: list[Capability.Scope] = []
        for scope_cls in sorted(intersection_scopes, key=lambda cls: cls.__name__):
            scopes = [
                scopes_by_cls[scope_cls] for scopes_by_cls in scopes_by_action.values() if scope_cls in scopes_by_cls
            ]
            if len(scopes) == 0:
                # Should not happen, as we already checked for intersection_scopes
                continue
            elif len(scopes) == 1:
                output_scopes.append(scopes[0])
            else:
                intersection: Capability.Scope | None = scopes[0]
                for scope in scopes[1:]:
                    intersection = scope_intersection(scope, intersection)
                    if intersection is None:
                        break
                if intersection is None:
                    # If the intersection is None, it means the scopes do not intersect
                    continue
                output_scopes.append(intersection)
        return output_scopes or None

    def _union_by_action_by_scope_cls(
        self, acl_cls: type[Capability], actions: set[Capability.Action]
    ) -> dict[Capability.Action, dict[type[Capability.Scope], Capability.Scope]]:
        scopes_by_action: dict[Capability.Action, dict[type[Capability.Scope], Capability.Scope]] = {}
        for action in actions:
            scopes_by_action[action] = {}
        for project_capability in self.token.capabilities:
            capability = project_capability.capability
            if not isinstance(capability, acl_cls):
                continue
            for action in capability.actions:
                if action in actions:
                    scopes_by_cls = scopes_by_action[action]
                    scope_cls = type(capability.scope)
                    scopes_by_cls[scope_cls] = scope_union(capability.scope, scopes_by_cls.get(scope_cls))
        return scopes_by_action
