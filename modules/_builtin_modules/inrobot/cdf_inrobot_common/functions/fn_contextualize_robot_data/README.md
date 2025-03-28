# Contextualize robot files

Function for contextualizing files that are uploaded from a robot to CDF. The function connects the file to
the correct asset, and connects the correct time series to the file. If no time series exist, they are created.
The function also assigns the correct action label to the file, indicating what action should be taken on the file,
e.g., read dial gauge, read valve.

List all files with metadata field `{"processed":"false"}` in input data set. If the image has an asset id in the
"asset_id"  metadata field, check that the asset exists and add that as "assetId" to the file.

If the image is a gauge reading image, check if the asset has a time series with label "GAUGE_TIME_SERIES". If it does,
add metadata field "ts_external_id" to the image.
If not, create the timeseries.

In this case we could write metadata to the time series if we are able to read it in the gauge reader service. So the
gauge reader would check if the TS has metadata or the file has metadata.

1. If the TS has metadata, use ts metadata.
2. If the TS does not have metadata and the file has metadata, use file metadata and write that metadata to TS.
3. If the TS does not have metadata and the file does not have metadata complete metadata, use incomplete metadata set
4. and read remainig metadata and write metadata to TS if successful

(Assume always complete or no metadata on timeseries)

With this we could instruct the user to take a very close and good picture of the gauge initially and that would work
in some cases. Optionally, the user could take en image of the gauge, add correct metadata in the vision app and
that metadata will be written to the timeseries.

It will not be easy to change the metadata if we read wrong metadata initially. An option could be to not not write
metadata to timeseries in case 3 (when metadata is read from image).
