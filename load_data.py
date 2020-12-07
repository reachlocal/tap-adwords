from google.cloud import bigquery

client = bigquery.Client(project='reachlocal-seo-development')

# adwords
# tables = ['AD_PERFORMANCE_REPORT', 'AGE_RANGE_PERFORMANCE_REPORT', 'CAMPAIGN_PERFORMANCE_REPORT',
#     'GENDER_PERFORMANCE_REPORT', 'KEYWORDS_PERFORMANCE_REPORT',
#     'PARENTAL_STATUS_PERFORMANCE_REPORT', 'PLACEHOLDER_FEED_ITEM_REPORT', 'PRODUCT_PARTITION_REPORT',
#     'SEARCH_QUERY_PERFORMANCE_REPORT', 'SHOPPING_PERFORMANCE_REPORT', 'STATS_BY_DEVICE_AND_NETWORK_REPORT',
#     'STATS_BY_DEVICE_HOURLY_REPORT', 'STATS_IMPRESSIONS_REPORT', 'STATS_WITH_SEARCH_IMPRESSIONS_REPORT',
#     'VIDEO_CAMPAIGN_PERFORMANCE_REPORT','VIDEO_PERFORMANCE_REPORT'
#     ]

# simplifi
tables = ['campaign_conversion_summary_reports', 'campaign_general_summary_reports', 'ad_summary_reports', 'ad_network_publisher_reports',
    'ad_geofence_reports', 'campaign_keyword_reports', 'campaign_geofence_reports', 'ad_device_reports', 'ad_conversion_reports',
    'campaign_network_publisher_reports', 'campaign_device_reports', 'ad_keyword_reports']
period = '2020-11_simplifi'

for table in tables:
    table_id = f'reachlocal-seo-development.simplifi.{table}'
    uri = f'gs://reachlocal-seo-development/{period}/{table}-*.csv'

    job_config = bigquery.LoadJobConfig(
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                max_bad_records=30)

    print(f'Loading files into {table}')
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    print(load_job.result())