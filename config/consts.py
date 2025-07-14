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
READ_EMAILS = './tmp/already_read.txt'
TEMP_PDF = './tmp/temp.pdf'
DOCS_PATH = './config/docs.json'
CSV_PATH = './tmp/xtb_export'
BUCKET_PATH = 'xtb_processed_df_dumps'
MAIL_IDS_PATH = 'xtb-gmail-read-email-id'
SERVICE_ACC = './config/xtb-ike-wallet-0a604e129e1a.json'
NBP_MID = 'a'
NBP_ASK_BID = 'c'
NBP_BASE_REQ = 'https://api.nbp.pl/api/exchangerates/rates/'
NBP_TMP_CSV = './tmp/nbp.csv'
NBP_BUCKET_PATH = 'nbp-usdpln-exchange-rates'
ALPHA_API = './config/alphavantage.json'
ALPHA_BASE_REQ = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'
ALPHA_EQ_SYMBOL_REQ = '&symbol='
ALPHA_OUTPUT_SIZE = '&outputsize='
ALPHA_OUTPUT_SIZE_TYPE = 'compact'
ALPHA_APIKEY_REQ = '&apikey='
ALPHA_FX_BASE_REQ = 'https://www.alphavantage.co/query?function=FX_DAILY'
ALPHA_FX_REQ_FROM_SYMBOL = '&from_symbol='
ALPHA_FX_REQ_TO_SYMBOL = '&to_symbol='
ALPHA_EQUITY_CSV = './tmp/equity.csv'
ALPHA_FX_CSV = './tmp/fx.csv'
ALPHA_MIN_MAX = './tmp/min_max.csv'
ALPHA_TICKER_SYMBOLS = []
ALPHA_FX = 0
ALPHA_EQ = 0
ALPHA_FX_BUCKET = 'alphavantage-fx-exchange-rates'
ALPHA_EQ_BUCKET = 'alphavantage-equit-quotes'
ALPHA_MIN_MAX_BUCKET = 'alphavantage-min-max-dates'
ALPHA_REQ_SYMBOLS = {
    # 'EIMI.LON': 'equity',
    # 'IWDA.LON': 'equity',
    'IGLN.LON': 'equity',
    'USDPLN': 'fx'
    }
ALPHA_REQ_TYPE = {'equity': ALPHA_BASE_REQ, 'fx': ALPHA_FX_BASE_REQ}
MERGED_DFS_NAMES = [
    'Numer zlecenia',
    'Symbol',
    'Nazwa instrumentu',
    'System wykonania zlecenia',
    'Wolumen', 
    'Data i czas transakcji',
    'Zlecenie otwarcia',
    'Cena jednostkowa',
    'Cena calkowita',
    'Klasa aktywow',
    'Waluta kwotowania instrumentu',
    'Kurs przewalutowania',
    'Koszty przewalutowania',
    'Prowizja',
    'Laczne koszty']
DATA_TYPES = {
    'Numer zlecenia': 'Int64',
    'Symbol': 'string',
    'Nazwa instrumentu': 'string',
    'System wykonania zlecenia': 'string',
    'Wolumen': 'Float64', 
    'Data i czas transakcji': 'datetime64[s]',
    'Zlecenie otwarcia': 'string',
    'Cena jednostkowa': 'Float64',
    'Cena calkowita': 'Float64',
    'Klasa aktywow': 'string',
    'Waluta kwotowania instrumentu': 'string',
    'Kurs przewalutowania': 'Float64',
    'Koszty przewalutowania': 'Float64',
    'Prowizja': 'Float64',
    'Laczne koszty': 'Float64'}