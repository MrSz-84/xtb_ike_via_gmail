import os, io, re, json, pytz, base64, pdfplumber, logging, pandas as pd
from datetime import datetime
from google.cloud import storage, secretmanager
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from config import consts as c


# with open('./config/token.json', mode='r', encoding='utf-8') as f:
#     os.environ['TOKEN_JSON'] = f.read()
    
# with open('./config/api_oauth.json', mode='r', encoding='utf-8') as f:
#     os.environ['CLIENT_SECRET_JSON'] = f.read()

# with open('./config/docs.json', mode='r', encoding='utf-8') as f:
#     os.environ['PDF_DECODE_KEY'] = f.read()
    
# with open('./config/xtb-ike-wallet-0a604e129e1a.json', mode='r', encoding='utf-8') as f:
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/xtb-ike-wallet-0a604e129e1a.json'


# logging.basicConfig(level=logging.ERROR)
DOCS = os.environ.get('PDF_DECODE_KEY').replace('"', '')
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
query = f'from:{c.SENDER} subject:{c.SUBJECT} has:attachment filename:{c.SPEC_ATTACHMENT} older_than:{c.OLDER_THAN} newer_than:{c.NEWER_THAN}'

def parse_date(raw):
    dt = datetime.strptime(raw, '%a, %d %b %Y %H:%M:%S %z')
    local_tz = pytz.timezone('Europe/Warsaw')
    local_dt = dt.astimezone(local_tz)
    iso_date = local_dt.isoformat()
    return iso_date

def export_date():
    dt = datetime.today()
    local_tz = pytz.timezone('Europe/Warsaw')
    dt = dt.astimezone(local_tz)
    dt = dt.strftime('%Y%m%d_%H%M%S')
    return dt

def parse_sender(to_parse):
    email = re.search(r'<(.*?)>', to_parse).group(1)
    return email

def read_emails_id_file(read_set):
    temp_set = read_set
    with open(c.READ_EMAILS, mode='a+', encoding='utf-8') as file:
        file.seek(0, os.SEEK_END)
        f_size = file.tell()
        if not f_size == 0:
            file.seek(0)
            for line in file:
                temp_set.add(line.strip())
    return temp_set

def write_emails_id_file(write_set):
    with open(c.READ_EMAILS, mode='w', encoding='utf-8') as f:
        for id in write_set:
            f.write(id+'\n')

def create_data_struct(data_batch, read_emails, emails_dct):
    if data_batch[0] not in read_emails:
        read_emails.add(data_batch[0])
        emails_dct[data_batch[0]] = {
            'date': data_batch[1], 
            'from': data_batch[2], 
            'to': data_batch[3], 
            'subject': data_batch[4],
            'attachment': {'name': data_batch[5], 'file': data_batch[6]}
            }
        
def replace_fractional(x):
    if 'ETF' in x:
        x = 'ETF'
    elif 'ETC' in x:
        x = 'ETC'
    return x

def merge_name_type(dfs_lst):
    merged = pd.concat(dfs_lst)
    merged.columns = c.MERGED_DFS_NAMES
    merged = merged.astype(c.DATA_TYPES)
    merged.reset_index(drop=True, inplace=True)
    return merged

def clean_dfs(df):
    cleaned = df
    cleaned.columns = range(len(cleaned.columns))
    cleaned.drop(columns=[0, 5], inplace=True)
    cleaned[3] = cleaned[3].str.replace('\n', ' ')
    cleaned[7] = cleaned[7].str.replace('\n', ' ')
    cleaned[11] = cleaned[11].apply(replace_fractional)
    return cleaned
        
def read_from_pdf(key, data_dct):
    df_to_process = []
    for val in data_dct.values():
        pdf = pdfplumber.open(io.BytesIO(val['attachment']['file']), password=key)
        for page in pdf.pages:
            table = page.extract_tables()
            table = pd.DataFrame(table[1], columns=table[0][0])
            df_to_process.append(clean_dfs(table))
    return merge_name_type(df_to_process)

def write_to_csv(df, csvname):
    df.to_csv(csvname, index=False, encoding='utf-8')
    
def upload_to_bucket(fname, gsbucket, dest_fname):
    dest_fname = dest_fname.replace('./tmp/','')
    client = storage.Client()
    bucket = client.bucket(gsbucket)
    blob = bucket.blob(dest_fname)
    blob.upload_from_filename(fname)
    print(f'⬆️ Upload of the file {fname} to {gsbucket} cloud storage bucket complete.')
    
def dowlnoad_from_bucket(source_fname, gsbucket, dest_fname):
    source_fname = dest_fname.replace('./tmp/','')
    client = storage.Client()
    bucket = client.bucket(gsbucket)
    blob = bucket.blob(source_fname)
    if blob.exists():
        print(f'ℹ️ Cloud Storage file object found. Downloading and writing...  {dest_fname}')
        blob.download_to_filename(dest_fname)
    else:
        print(f'⬇️ Cloud Storage file object not found. Creating empty file...  {dest_fname}')
        write_emails_id_file(set())

def check_credentials():
    creds_info = json.loads(os.environ['TOKEN_JSON'])
    credentials = Credentials.from_authorized_user_info(creds_info, SCOPES)
    
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
        # sm = secretmanager.SecretManagerServiceClient()
        # parent = 'projects/xtb-ike-wallet/secrets/gmail-token'
        # sm.addsecret_version(
        #     request = {'parent': parent,
        #                'payload': {'data': credentials.to_json().encode}}
        # )
    return credentials

def get_messages_details(service, emails_dct, read_emails, messages):
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
        msg_id = msg['id']
        headers = msg['payload']['headers']
        
        sender = next((header['value'] for header in headers if header['name'] == 'From'), 'Brak nadawcy')
        sender = parse_sender(sender)
        to = next((header['value'] for header in headers if header['name'] == 'To'), 'Brak odbiorcy')
        subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'Brak tematu')
        date = next((header['value'] for header in headers if header['name'] == 'Date'), 'Brak daty')
        date = parse_date(date)
        
        if 'parts' in msg['payload']:
            for part in msg['payload']['parts']:
                if 'filename' in part and part['filename']:
                    if 'body' in part and 'attachmentId' in part['body']:
                        attachment = service.users().messages().attachments().get(
                            userId='me', messageId=message['id'], id=part['body']['attachmentId']
                        ).execute()
                        file_data = attachment['data']
                        file_name = part['filename']
                        file_bytes = base64.urlsafe_b64decode(file_data)
        batch = [msg_id, date, sender, to, subject, file_name, file_bytes]
        create_data_struct(batch, read_emails, emails_dct)
    return emails_dct

def main():
    emails_dct = {}
    read_emails = read_emails_id_file(set())
    
    creds = check_credentials()
    print('▶️ Start – ...  ✅ Gmail credentials loaded')
        
    try:
        service = build(c.API_NAME, c.API_VERSION, credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=13, q=query).execute()
        messages = results.get('messages', [])
    except HttpError as error:
        print(f'An error occured: {error}')
        
    if not messages:
        print('ℹ️ No messages found. Finishing program...')
        os.remove(c.READ_EMAILS)
        return

    emails_dct = get_messages_details(service, emails_dct, read_emails, messages)
    if not bool(emails_dct):
        print('ℹ️ No new messages to process. Finishing program...')
        os.remove(c.READ_EMAILS)
        return
    merged_dfs = read_from_pdf(DOCS, emails_dct)
    csv_name = f'{c.CSV_PATH}{export_date()}.csv'
    write_to_csv(merged_dfs, csv_name)
    try:
        upload_to_bucket(csv_name, c.BUCKET_PATH, csv_name)
    except Exception as e:
        os.remove(csv_name)
        print(e)
    write_emails_id_file(read_emails)
    try:
        upload_to_bucket(c.READ_EMAILS, c.MAIL_IDS_PATH, c.READ_EMAILS)
    except Exception as e:
        os.remove(c.READ_EMAILS)
        print(e)
        
    os.remove(c.READ_EMAILS)
    os.remove(csv_name)
    print('✅ All tasks done, finishing program...')

if __name__ == "__main__":
    main()
