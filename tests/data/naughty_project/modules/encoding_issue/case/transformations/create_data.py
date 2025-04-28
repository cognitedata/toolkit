from pathlib import Path

raw = """name: encoding_confusion
query: >-
    SELECT * FROM my_éñcüd€d£d_table
    WHERE column = 'value'
destination:
  type: assets
ignoreNullFields: true
isPublic: true
conflictMode: upsert
dataSetExternalId: ds_at_p66
"""

THIS_FOLDER = Path(__file__).resolve(strict=True).parent
encodings = ["utf-8", "utf-16", "cp1252"]
for encoding in encodings:
    file = THIS_FOLDER / f"my_{encoding}.Transformation.yaml"
    file.write_text(f"externalId: encoding_transformation_{encoding}\n{raw}", encoding=encoding)
