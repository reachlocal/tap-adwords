#!/usr/bin/env python3
import os
import simplejson as json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


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

        get_report(stream.tap_stream_id, config, schema)
        singer.write_state({"last_updated_at": datetime.now().isoformat()})
    return

def get_report(stream, config, schema):
    access_token = get_token(config)
    if (stream == "STATS_BY_DEVICE_AND_NETWORK_REPORT" or stream == "STATS_BY_DEVICE_HOURLY_REPORT" or stream == "STATS_WITH_SEARCH_IMPRESSIONS_REPORT" or
            stream == "STATS_IMPRESSIONS_REPORT" or stream == "VIDEO_CAMPAIGN_PERFORMANCE_REPORT"):
        stream = "CAMPAIGN_PERFORMANCE_REPORT"

    fields = list(schema["properties"].keys())[1:]
    props = [(k, v) for k, v in schema["properties"].items()][1:]

    predicate = ""
    if stream == "PLACEHOLDER_FEED_ITEM_REPORT":
        predicate = "WHERE PlaceholderType IN [2, 17, 7, 24, 1, 35, 31] AND Impressions != 0"

    payload={
        '__rdquery': f'SELECT {", ".join(fields)} FROM {stream} {predicate} DURING YESTERDAY',
        '__fmt': 'CSV'}

    customers = get_customers(access_token, config)
    if len(customers) > 1:
        customers = list(filter(lambda x: x != config["customerId"], customers))

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda cust_idx: process_customer(cust_idx, customers, config, payload, props, access_token, stream), range(len(customers)))

def process_customer(cust_idx, customers, config, payload, props, access_token, stream):
    customer = customers[cust_idx]
    headers = {
        'developerToken': config["developerToken"],
        'Authorization': f'Bearer {access_token}',
        'clientCustomerId': customer,
        'skipReportHeader': "true",
        'skipReportSummary': "true",
        'skipColumnHeader': "true"
    }
    url = "https://adwords.google.com/api/adwords/reportdownload/v201809"

    resp = requests.post(url, headers=headers, data=payload, files=[])
    lines = resp.text.splitlines(False)
    for line in lines:
        items = line.split(',')
        obj = {
            "AdvertiserId": int(customer)
        }
        for index in range(len(items)):
            value = items[index]
            if props[index][1]["type"] == "number":
                value = float(value) if "." in value else int(value)
            obj[props[index][0]] = value
        singer.write_record(stream, obj)
    LOGGER.info(f'[{stream}] Customer {cust_idx + 1}/{len(customers) - 1} processed')

def get_token(config):
    token_url = "https://www.googleapis.com/oauth2/v4/token"
    payload = f'grant_type=refresh_token&refresh_token={config["refreshToken"]}'
    token_headers = {
        'Authorization': f'Basic {config["auth"]}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", token_url, headers=token_headers, data=payload)
    return response.json()["access_token"]

def get_customers(access_token, config):
    headers = {
        'developer-token': config["developerToken"],
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    body = {
        "query": "SELECT customer_client.id FROM customer_client"
    }
    url = f'https://googleads.googleapis.com/v4/customers/{int(config["customerId"])}/googleAds:searchStream'
    resp = requests.post(url, headers=headers, data=json.dumps(body)).json()
    return list(map(lambda x: x["customerClient"]["id"], resp[0]["results"]))

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
