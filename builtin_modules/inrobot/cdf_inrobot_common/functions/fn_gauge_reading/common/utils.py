import io

from PIL import Image


def batch(iterable, n=1):
    """Batch iterable."""
    len_iterable = len(iterable)
    for ndx in range(0, len_iterable, n):
        yield iterable[ndx : min(ndx + n, len_iterable)]


def image_to_byte_array(image: Image) -> bytes:
    """Convert PIL image to byte array."""
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format=image.format)
    return img_byte_arr.getvalue()
