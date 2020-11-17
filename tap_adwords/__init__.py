#!/usr/bin/env python3
import os
import simplejson as json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime


REQUIRED_CONFIG_KEYS = []
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        schema = utils.load_json(get_abs_path("schemas/{}.json".format(stream.tap_stream_id)))
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        data = get_report(stream.tap_stream_id, config, schema)

        for row in data:
            singer.write_records(stream.tap_stream_id, [row])
        singer.write_state({"last_updated_at": datetime.now().isoformat()})
    return

def get_report(stream, config, schema):
    fields = list(schema["properties"].keys())
    url = "https://adwords.google.com/api/adwords/reportdownload/v201809"

    payload={
        '__rdquery': f'SELECT {", ".join(fields)} FROM {stream} DURING LAST_30_DAYS',
        '__fmt': 'CSV'}
    files=[

    ]
    headers = {
        'developerToken': config["developerToken"],
        'Authorization': f'Bearer {get_token(config)}',
        'clientCustomerId': config["customerId"],
        'skipReportHeader': "true",
        'skipReportSummary': "true",
        'skipColumnHeader': "true"
    }

    resp = requests.post(url, headers=headers, data=payload, files=files)
    lines = resp.text.splitlines(False)
    result = []
    for line in lines:
        items = line.split(',')
        obj = {}
        for index in range(len(items)):
            obj[fields[index]] = items[index]
        result.append(obj)

    return result

def get_token(config):
    token_url = "https://www.googleapis.com/oauth2/v4/token"
    payload = f'grant_type=refresh_token&refresh_token={config["refreshToken"]}'
    token_headers = {
    'Authorization': f'Basic {config["auth"]}',
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", token_url, headers=token_headers, data=payload)
    return response.json()["access_token"]


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
