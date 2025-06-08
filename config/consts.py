SENDER ='dailystatements@mail.xtb.com'
SUBJECT = 'Potwierdzenie wykonania zleceń'
ATTACHMENT = True
SPEC_ATTACHMENT = 'pdf'
OLDER_THAN = '0d' 
NEWER_THAN = '1m'
TOKEN = './config/token.json'
SECRET = './config/api_oauth.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
READ_EMAILS = './files/already_read.txt'
TEMP_PDF = './files/temp.pdf'
DOCS_PATH = './config/docs.json'
CSV_PATH = './files/xtb_export'
BUCKET_PATH = 'xtb_processed_df_dumps'
SERVICE_ACC = './config/xtb-ike-wallet-0a604e129e1a.json'
MERGED_DFS_NAMES = [
    'Numer zlecenia',
    'Symbol',
    'Nazwa instrumentu',
    'System wykonania zlecenia',
    'Wolumen', 
    'Data i czas transakcji',
    'Zlecenie otwarcia',
    'Cena jednostkowa',
    'Cena całkowita',
    'Klasa aktywów',
    'Waluta kwotowania instrumentu',
    'Kurs przewalutowania',
    'Koszty przewalutowania',
    'Prowizja',
    'Łączne koszty']
DATA_TYPES = {
    'Numer zlecenia': 'Int64',
    'Symbol': 'string',
    'Nazwa instrumentu': 'string',
    'System wykonania zlecenia': 'string',
    'Wolumen': 'Float64', 
    'Data i czas transakcji': 'datetime64[s]',
    'Zlecenie otwarcia': 'string',
    'Cena jednostkowa': 'Float64',
    'Cena całkowita': 'Float64',
    'Klasa aktywów': 'string',
    'Waluta kwotowania instrumentu': 'string',
    'Kurs przewalutowania': 'Float64',
    'Koszty przewalutowania': 'Float64',
    'Prowizja': 'Float64',
    'Łączne koszty': 'Float64'}