"""Migration of accessManagement field for InFieldLocationConfig."""

from typing import Any

from ..types_new import AccessManagement


def migrate_access_management(location_dict: dict[str, Any]) -> AccessManagement | None:
    """Migrate accessManagement from old configuration.

    Extracts templateAdmins and checklistAdmins from the old location configuration
    and creates an AccessManagement dict.

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        AccessManagement dict, or None if neither templateAdmins nor checklistAdmins are present
    """
    template_admins = location_dict.get("templateAdmins")
    checklist_admins = location_dict.get("checklistAdmins")

    # Only create accessManagement if at least one of the fields is present
    if not template_admins and not checklist_admins:
        return None

    access_management: AccessManagement = {}
    if template_admins:
        access_management["templateAdmins"] = template_admins
    if checklist_admins:
        access_management["checklistAdmins"] = checklist_admins

    return access_management

