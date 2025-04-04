# Gauge reading

Uses the API endpoint.
Takes all files with the specified label and reads an analog gauge in the image.

## Input parameters

- *gauge_type*: The gauge type to use in the API call
- *input_label*: Label to list files by
- *output_label*: Label for processed files.
- *success_label*: The label added to files where the gauge reading was successful.
- *failed_label*: The label added to files where the gauge reading has failed.

## Gauge reading

- The function performs gauge reading on all files with the input label defined in the schedule file.
- Each input file can contain none, some of or all the metadata fields:

  - min_level
  - max_level
  - unit
  - dead_angle
  - ts_external_id

- If the file has the ts_external_id metadata field, we first try to use the metadata from the time series as input
  metadata (min_level, max_level, unit, dead_angle). If the ts do not contain all these fields and the file contains
  a complete metadata set (min_level, max_level, unit, dead_angle), the timeseries is updated with the correct metadata.
- If the file does not have the ts_external_id metadata field or no complete metadata set is found, the existing file
  metadata fields are used as input metadata to the API. For exmaple if the file has a metadata field "unit",
  but not "min_level" etc, only unit is given as input to the API.
- If the reading returns a complete metadata reading and the timeseries did not have metadata, the time series is
  updated with metadata.
- After the file is read the input_label is removed and the output_label is added.
- If the reading fails, the failed_label is added to the image.
- If the reading succeeds, the success_label is added to the image and value and metadata is added til file metadata.
- If the file contains the ts_external_id field in metadata, the value is written to the timeseries.
- If the reading fails, all attributes that are read are still written to the image. If for example unit and min_level
- is read from the image, unit and min_level are added in the file metadata.

## Lables

- The function checks that all labels exist, and creates the fields that do not exist.
- The funtion removes the input label from the image and adds the output label for processed files to the file
  as well as the success or fail label.

## Timestamp

Timestamp is found from

1. Metadata field called timestamp
2. Source created time
3. Uploaded time

## Example request and response from the gauge reading endpoint

[cognitedata/context-api-workers/pull/981](https://github.com/cognitedata/context-api-workers/pull/981)
