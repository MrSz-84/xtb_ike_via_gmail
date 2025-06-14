import functions_framework, datetime, traceback, logging
from google.cloud import bigquery
from google.api_core import exceptions


@functions_framework.cloud_event
def process_file(cloudevent):
    file = cloudevent.data['name']    
    bucket = cloudevent.data['bucket']
    time = cloudevent.data['timeCreated']
    TABLE_ID_STR = 'xtb-ike-wallet.retirement_portfolio.xtb_transactions_import_'
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
    
    def table_id_dynamic_date(datetime_str, table):
        try:
            dt_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            return table + 'ERROR'
        dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
        import_date = dt_obj.strftime('%Y%m%d')
        return table + import_date
    
    def create_table_if_not_exist(table_schema, table_prefix, time_str, filename, bucketname):
        client = bigquery.Client()
        table_id = table_id_dynamic_date(time_str, table_prefix)
        table_ref = bigquery.Table(table_id, schema=table_schema)
        
        try:
            client.get_table(table_ref)
            print(f'ℹ️ Table {table_id} exists - TRUNCATE')
            query = f'TRUNCATE TABLE `{table_id}`'
            client.query(query).result()
        except exceptions.NotFound:
            print(f'ℹ️ Creating table {table_id}')
            client.create_table(table_ref)
        
        uri = f'gs://{bucketname}/{filename}'
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )
        print(f'⬆️ Uploading {uri} to {table_id}')
        client.load_table_from_uri(uri, table_id, job_config=job_config).result()
    
    
    print(f"▶️ Start – {file} ({time})")
    try:    
        create_table_if_not_exist(SCHEMA, TABLE_ID_STR, time, file, bucket)
        print("✅ Finished successfully")
    except Exception as e:
        logging.error("❌ Main error: %s\n%s", e, traceback.format_exc())
        raise
