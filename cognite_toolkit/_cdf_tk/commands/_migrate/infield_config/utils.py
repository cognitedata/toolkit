"""Utility functions for InField V2 config migration."""

import uuid
from typing import Any


def get_original_external_id(location_dict: dict[str, Any]) -> str:
    """Extract the original external ID from a location configuration dict.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        
    Returns:
        Original external ID (externalId, assetExternalId, or generated UUID)
    """
    return (
        location_dict.get("externalId")
        or location_dict.get("assetExternalId")
        or str(uuid.uuid4())
    )


def get_location_filter_external_id(location_dict: dict[str, Any]) -> str:
    """Generate the LocationFilter external ID with prefix.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        
    Returns:
        Location filter external ID with "location_filter_" prefix
    """
    original_external_id = get_original_external_id(location_dict)
    return f"location_filter_{original_external_id}"


def get_location_config_external_id(location_dict: dict[str, Any], index: int) -> str:
    """Generate the InFieldLocationConfig external ID.
    
    If externalId exists, use it directly. If only assetExternalId exists,
    add index postfix to ensure uniqueness. Otherwise, generate a UUID.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        index: Index of the location in the list (for uniqueness when only assetExternalId exists)
        
    Returns:
        Location config external ID
    """
    if location_dict.get("externalId"):
        return location_dict["externalId"]
    elif location_dict.get("assetExternalId"):
        # Add index postfix to ensure uniqueness when only assetExternalId is available
        return f"{location_dict['assetExternalId']}_{index}"
    else:
        return f"infield_location_{str(uuid.uuid4())}"

