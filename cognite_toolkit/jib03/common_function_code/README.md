# Common function code

This is an example of having a common directory for function code.
You import the code from the common package, i.e. `from common.tool import CDFClientTool`

The CDFClientTool is a helper wrapper around CogniteClient to make it easire to run
code locally and validate that you have the capabilities necessary for the function to run.
