import json
from pathlib import Path

text = Path("_tmp_redoc_state.js").read_text(encoding="utf-8")
start = text.index('JSON.parse("') + len('JSON.parse("')
end = text.rindex('");')
decoded = json.loads('"' + text[start:end] + '"')
payload = json.loads(decoded)
spec = payload["definition"]["data"]["versions"][0]["spec"]
schemas = spec["components"]["schemas"]
print("schema count", len(schemas))

interesting = []
for name, schema in schemas.items():
    blob = json.dumps(schema)
    if any(token in blob for token in ("queryParam", "queryParameter", "hostedextractor", "HostedExtractor")):
        interesting.append(name)
    lowered = name.lower()
    if any(
        token in lowered
        for token in ("source", "job", "destination", "mapping", "incremental", "mqtt", "kafka", "rest")
    ):
        if "extractor" in lowered or "hosted" in lowered or "incremental" in lowered or "jobformat" in lowered:
            interesting.append(name)

wanted = [
    "CreateDestination",
    "Destination",
    "CreateJob",
    "MinimalJob",
    "MQTTJobConfig",
    "KafkaJobConfig",
    "QueryParameterConfig",
    "HeaderValueConfig",
    "NextUrlConfig",
    "BodyPaginationConfig",
    "JobConfig",
    "CreateMapping",
    "Mapping",
    "CustomMapping",
    "CreateEventHubSource",
    "CreateKafkaSource",
    "CreateMqttBrokerSource",
    "CreateMqttV3Source",
    "CreateMqttV5Source",
    "CreateRestSource",
    "EventHubSource",
    "KafkaSource",
    "MqttBrokerSource",
    "MqttV3Source",
    "MqttV5Source",
    "RestSource",
    "JobFormat",
]
# Find format schemas
format_names = [
    name
    for name in schemas
    if "Format" in name
    and (
        "Job" in name
        or name in {"CogniteFormat", "CustomFormat", "RockwellFormat", "ValueFormat", "Prefix", "DataModels"}
    )
]
print("FORMAT", format_names)
print("PATHS", [p for p in spec["paths"] if "hostedextract" in p])
extra = [
    "HostedExtractorMetadata",
    "SparkplugFormat",
    "WriteCustomFormat",
    "EncodingType",
    "CompressionType",
    "PrefixConfig",
    "DataModels",
    "JobConfig",
    "UpdateDestination",
    "UpdateMapping",
    "UpdateJob",
    "JobUpdateItem",
    "DestinationUpdateItem",
    "MappingUpdateItem",
    "CreateInputConfig",
    "InputConfig",
    "CSVInput",
    "ProtobufInput",
]
print("EXTRA NAMES")
for name in extra:
    print(name, name in schemas)
print("--- lookup similar ---")
for token in ["UpdateJob", "JobUpdate", "Sparkplug", "WriteCustom", "HostedExtractorMetadata", "CSV", "Protobuf"]:
    print(token, [n for n in schemas if token.lower() in n.lower()][:30])


def summarize(name: str) -> None:
    schema = schemas[name]
    print(f"\n===== {name} =====")
    print("required", schema.get("required"))
    props = schema.get("properties") or {}
    if "$ref" in schema:
        print("ref", schema["$ref"])
    if "allOf" in schema:
        print("allOf", json.dumps(schema["allOf"])[:500])
    for prop, spec_ in props.items():
        compact = {
            k: spec_[k]
            for k in spec_
            if k
            in {
                "type",
                "enum",
                "minLength",
                "maxLength",
                "minimum",
                "maximum",
                "maxProperties",
                "minItems",
                "maxItems",
                "default",
                "pattern",
                "format",
                "x-maxKeyLength",
                "description",
                "title",
            }
        }
        if "$ref" in spec_:
            compact["$ref"] = spec_["$ref"]
        if "oneOf" in spec_ or "anyOf" in spec_ or "discriminator" in spec_:
            compact["union"] = spec_.get("discriminator") or spec_.get("oneOf") or spec_.get("anyOf")
        print(f"  {prop}: {json.dumps(compact, ensure_ascii=False)[:500]}")


print("PATHS")
for path in spec["paths"]:
    if "hostedextract" in path:
        print(path, list(spec["paths"][path]))

focus = [
    "HostedExtractorMetadata",
    "SparkplugFormat",
    "WriteCustomFormat",
    "EncodingType",
    "CompressionType",
    "PrefixConfig",
    "DataModels",
    "JobConfig",
    "UpdateDestination",
    "UpdateMapping",
    "CSVInputConfig",
    "CreateCSVInput",
]
print("\nNAME MATCHES")
for token in [
    "UpdateJob",
    "JobUpdate",
    "Sparkplug",
    "WriteCustom",
    "HostedExtractorMetadata",
    "InputConfig",
    "CSV",
    "Protobuf",
    "UpdateMqtt",
    "UpdateRest",
    "UpdateKafka",
    "UpdateEvent",
]:
    print(token, [n for n in schemas if token.lower() in n.lower()][:40])

for name in focus:
    if name not in schemas:
        print("MISSING", name)
        continue
    print("\nFULL", name)
    print(json.dumps(schemas[name], indent=2)[:2500])
