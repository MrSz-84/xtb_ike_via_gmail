import functions_framework, traceback, logging
from google.cloud import bigquery
from google.api_core import exceptions


@functions_framework.cloud_event
def process_file(cloudevent):
    file = cloudevent.data['name']    
    bucket = cloudevent.data['bucket']
    time = cloudevent.data['timeCreated']
    TABLE_ID_STR = 'xtb-ike-wallet.retirement_portfolio.xtb_transactions_import'
    SCHEMA = [
        bigquery.SchemaField('NumerZlecenia', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('Symbol', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('NazwaInstrumentu', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('SystemWykZlecenia', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Wolumen', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('DataCzas', 'DATETIME', mode='REQUIRED'),
        bigquery.SchemaField('ZlecenieOtwarcia', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('CenaJedn', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('CenaCalk', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('KlasaAktywow', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Waluta', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('KursPrzewalutowania', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('KosztPrzewalutowania', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('Prowizja', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('LaczneKoszty', 'FLOAT', mode='REQUIRED'),
    ]
    
    def create_table_if_not_exist(table_schema, table_prefix, filename, bucketname):
        client = bigquery.Client()
        table_id = table_prefix
        table_ref = bigquery.Table(table_id, schema=table_schema)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='DataCzas'
        )
        table_ref.clustering_fields = ['Symbol', 'Waluta']
        
        try:
            client.get_table(table_ref)
            print(f'ℹ️ Table {table_id} exists')
        except exceptions.NotFound:
            print(f'ℹ️ Creating table {table_id}')
            client.create_table(table_ref)
        
        uri = f'gs://{bucketname}/{filename}'
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema = table_schema,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        )
        print(f'⬆️ Uploading {uri} to {table_id}')
        client.load_table_from_uri(uri, table_id, job_config=job_config).result()
    
    
    print(f"▶️ Start – {file} ({time})")
    try:    
        create_table_if_not_exist(SCHEMA, TABLE_ID_STR, file, bucket)
        print("✅ Finished successfully")
    except Exception as e:
        logging.error("❌ Main error: %s\n%s", e, traceback.format_exc())
        raise
