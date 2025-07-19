import io
import PIL



def batch(iterable, n=1):
    """Batch iterable."""
    len_iterable = len(iterable)
    for ndx in range(0, len_iterable, n):
        yield iterable[ndx : min(ndx + n, len_iterable)]


def image_to_byte_array(image: PIL.Image) -> bytes:
    """Convert PIL image to byte array."""
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format=image.format)
    return img_byte_arr.getvalue()


def get_custom_mapping_metadata_dicts(data: dict[str, str]) -> tuple[dict[str, str], dict[str, str], bool]:
    """From input data check if there are any custom_metadata_ fields that need to be used when generating data"""

    include_custom_metadata = False  # Return False if there are no custom_metadata_ fields defined
    custom_metadata_dict = {}  # Dictionary of custom metadata fields and default values
    custom_mapping_dict = {}  # Dictionary of custom metadata fields to custom metadata field names if required

    if any(key.startswith("custom_metadata_") for key in data.keys()):
        include_custom_metadata = True
        custom_metadata_dict = {key: value for key, value in data.items() if key.startswith("custom_metadata_")}
        custom_mapping_dict = {
            key.replace("custom_mapping_", "custom_metadata_"): value
            for key, value in data.items()
            if key.startswith("custom_mapping_")
        }

    # custom_metadata_dict, custom_mapping_dict, include_custom_metadata = get_custom_mapping_metadata_dicts(data)
    return custom_metadata_dict, custom_mapping_dict, include_custom_metadata


def rename_custom_metadata_fields_with_custom_names(
    data_dict: dict[str, str], mapping_dict: dict[str, str]
) -> dict[str, str]:
    """
    return new metadata dictionary given a custom_metadata_dictionary: data_dict
    and a custom_mapping_: mapping_dict
    dictionary defined in the schedule.yaml file
    """

    return {mapping_dict[key]: value if key in mapping_dict else value for key, value in data_dict.items()}