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
import threading


REQUIRED_CONFIG_KEYS = []
LOGGER = singer.get_logger()
access_token = ''

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    timer = threading.Timer(sec, func_wrapper)
    timer.start()
    return timer

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

        full_path = "schemas/{}.json".format(stream.tap_stream_id.lower())
        schema = utils.load_json(get_abs_path(full_path))
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        get_token(config)
        interval = set_interval(lambda: get_token(config), 3500)
        get_report(stream.tap_stream_id, config, schema)
        interval.cancel()
        singer.write_state({"last_updated_at": str(datetime.now().isoformat()), "stream": stream.tap_stream_id})
    return

def get_report(stream, config, schema):
    reporting_table = stream
    if (stream == "STATS_BY_DEVICE_AND_NETWORK_REPORT" or stream == "STATS_BY_DEVICE_HOURLY_REPORT" or stream == "STATS_WITH_SEARCH_IMPRESSIONS_REPORT" or
            stream == "STATS_IMPRESSIONS_REPORT" or stream == "VIDEO_CAMPAIGN_PERFORMANCE_REPORT"):
        reporting_table = "CAMPAIGN_PERFORMANCE_REPORT"

    fields = list(schema["properties"].keys())[2:]
    props = [(k, v) for k, v in schema["properties"].items()][2:]

    predicate = ""
    if stream == "PLACEHOLDER_FEED_ITEM_REPORT":
        predicate = "WHERE PlaceholderType IN [2, 17, 7, 24, 1, 35, 31] AND Impressions != 0"

    payload={
        '__rdquery': f'SELECT {", ".join(fields)} FROM {reporting_table} {predicate} DURING {config["dateRange"]}',
        '__fmt': 'CSV'
    }

    customers = get_customers(config)
    if len(customers) > 1:
        customers = list(filter(lambda x: x["masterId"] != x["customerId"], customers))

    with ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(lambda arg: process_customer(arg[0], arg[1], len(customers), config, payload, props, stream), enumerate(customers))

def process_customer(cust_idx, customer, total, config, payload, props, stream):
    global access_token
    headers = {
        'developerToken': config["developerToken"],
        'Authorization': f'Bearer {access_token}',
        'clientCustomerId': str(customer["customerId"]),
        'skipReportHeader': "true",
        'skipReportSummary': "true",
        'skipColumnHeader': "true"
    }
    url = "https://adwords.google.com/api/adwords/reportdownload/v201809"

    resp = requests.post(url, headers=headers, data=payload, files=[])
    if resp.status_code != 200:
        LOGGER.info(f'Request failed for customer: {customer["customerId"]}')
    lines = resp.text.splitlines(False)
    for line in lines:
        items = line.split(',')
        obj = {
            "MasterAdvertiserId": int(customer["masterId"]),
            "AdvertiserId": int(customer["customerId"])
        }
        for index in range(len(items)):
            value = items[index]
            if props[index][1]["type"] == "number" or props[index][1]["type"] == "integer":
                value = float(value) if "." in value else int(value)
            obj[props[index][0]] = str(value)
        singer.write_record(stream, obj)
    LOGGER.info(f'[{stream}] Customer {cust_idx + 1}/{total - 1} processed')

def get_token(config):
    token_url = "https://www.googleapis.com/oauth2/v4/token"
    payload = f'grant_type=refresh_token&refresh_token={config["refreshToken"]}'
    token_headers = {
        'Authorization': f'Basic {config["auth"]}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", token_url, headers=token_headers, data=payload)
    LOGGER.info('Token refreshed')
    global access_token
    access_token = response.json()["access_token"]

def get_customers(config):
    global access_token
    headers = {
        'developer-token': config["developerToken"],
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'login-customer-id': config["rootCustomerId"]
    }
    body = {
        "query": "SELECT customer_client.id FROM customer_client"
    }

    root_mccs = config["customerId"].split(',')
    results = []
    for root_mcc in root_mccs:
        url = f'https://googleads.googleapis.com/v4/customers/{int(root_mcc)}/googleAds:searchStream'
        resp = requests.post(url, headers=headers, data=json.dumps(body)).json()
        for entry in resp:
            results.extend(list(map(lambda x: { "masterId": root_mcc, "customerId": x["customerClient"]["id"] }, entry["results"])))
    return results

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
