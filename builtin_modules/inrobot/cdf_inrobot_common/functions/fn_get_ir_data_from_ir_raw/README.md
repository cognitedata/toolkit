# Get IR data from IR raw files

Function for converting the raw IR files that are uploaded from Spot into IR images (RGB images with colour based on
temperature) and temperature files (CSV files where one cell corresponds to one pixel value, and contains the
temperature of that pixel value).

The raw IR files are read from CDF, and after conversion, the IR images and temperature files are uploaded to CDF.
The raw IR files are kept in CDF after conversion.

## Required parameters

- `input_label`: the raw IR files get this label when they are uploaded from Spot. The label is used to list the raw
  IR files. By default, this label is set to `read_ir`.
- `output_label`: the raw IR files get this label when the conversion into IR images and temperature files are finished.
  By default, this label is set to `ir_finished`.
- `success_label`: the raw IR files get this label if the conversion was successful.
- `failed_label`: the raw IR files get this label if the conversion failed.
- `data_set_id`: the ID of the dataset that the function reads the files from.
