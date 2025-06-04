SENDER ='dailystatements@mail.xtb.com'
SUBJECT = 'Potwierdzenie wykonania zleceń'
ATTACHMENT = True
SPEC_ATTACHMENT = 'pdf'
OLDER_THAN = '0d' 
#(1, 'day')
NEWER_THAN = '1m'
#(1, 'month')
TOKEN = './config/token.json'
SECRET = './config/api_oauth.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
READ_EMAILS = './files/already_read.txt'