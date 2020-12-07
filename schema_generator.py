import json

stream = 'VIDEO_PERFORMANCE_REPORT'
table = f'adwords.{stream}'

field_map = {
    "integer": "INT64",
    "string": "STRING",
    "number": "FLOAT64",
    "boolean": "BOOL"
}

schema = {}
with open(f'tap_adwords/schemas/{stream}.json') as f:
  schema = json.load(f)

props = []
for attr, value in schema["properties"].items():
    if "format" in value:
        props.append(f'{attr} DATE')
    else:
        props.append(f'{attr} {field_map[value["type"]]}')

query = f'CREATE TABLE {table} ({",".join(props)}) PARTITION BY Date'
print(query)
