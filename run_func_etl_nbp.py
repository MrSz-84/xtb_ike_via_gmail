import functions_framework, traceback, logging
from google.cloud import bigquery
from google.api_core import exceptions

bq_client = bigquery.Client()
TABLE_ID_STR = 'xtb-ike-wallet.retirement_portfolio.nbp_usdpln_exchange_rates'
SCHEMA = [
    bigquery.SchemaField('Date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('Currency', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('mid', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('ask', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('bid', 'FLOAT', mode='REQUIRED'),
    ]

@functions_framework.cloud_event
def process_file(cloudevent):
    data = cloudevent.data
    filename = data.get('name')
    bucket = data.get('bucket')
    
    if not filename or not bucket:
        logging.error('❌ Missing \'name\' or \'bucket\' in event data.')
        return
    
    if not  filename.lower().endswith('.csv'):
        logging.info('ℹ️ Skipping non-csv file: %s', filename)
        return
    
    uri = f'gs://{bucket}/{filename}'
    table_ref = bigquery.Table(TABLE_ID_STR, schema=SCHEMA)
    
    try:
        try:
            bq_client.get_table(table_ref)
            logging.info('ℹ️ Table %s exists', TABLE_ID_STR)
        except exceptions.NotFound:
            logging.info('ℹ️ Creating table %s', TABLE_ID_STR)
            bq_client.create_table(table_ref)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema = SCHEMA,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            )
        load_job = bq_client.load_table_from_uri(uri, TABLE_ID_STR, job_config=job_config)
        result = load_job.result()
        logging.info('✅ Loaded %d rows into %s', result.output_rows, TABLE_ID_STR)
    except Exception as e:
        logging.error("❌ Error processing %s: %s\n%s", filename, e, traceback.format_exc())
        raise
