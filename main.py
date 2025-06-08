import os
import io
import re
import json
import pytz
import base64
import tabula
import pandas as pd
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from PyPDF2 import PdfReader, PdfWriter
from config import consts as c


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
query = f'from:{c.SENDER} subject:{c.SUBJECT} has:attachment filename:{c.SPEC_ATTACHMENT} older_than:{c.OLDER_THAN} newer_than:{c.NEWER_THAN}'

with open(c.DOCS_PATH, mode='r', encoding='utf-8') as docs:
    docs = json.load(docs)


def parse_date(raw):
    dt = datetime.strptime(raw, '%a, %d %b %Y %H:%M:%S %z')
    local_tz = pytz.timezone('Europe/Warsaw')
    local_dt = dt.astimezone(local_tz)
    iso_date = local_dt.isoformat()
    return iso_date

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

def check_credentials(credentials):
    if os.path.exists(c.TOKEN):
        credentials = Credentials.from_authorized_user_file(c.TOKEN, SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                c.SECRET, SCOPES
            )
            credentials = flow.run_local_server(port=0)
        with open(c.TOKEN, 'w') as token:
            token.write(credentials.to_json())
    return credentials

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
    cleaned.drop(index=0, inplace=True)
    cleaned.reset_index(inplace=True, drop=True)
    cleaned.drop(columns=[0, 1, 3, 5, 7, 9, 10, 11, 13, 15, 18, 20, 22, 24, 26, 28, 30, 32], inplace=True)
    cleaned[6] = cleaned[6].str.replace('\r', ' ')
    cleaned[14] = cleaned[14].str.replace('\r', ' ')
    cleaned[21] = cleaned[21].apply(replace_fractional)
    return cleaned
        
def read_from_pdf(key, data_dct):
    df_to_process = []
    for val in data_dct.values():
        reader = PdfReader(io.BytesIO(val['attachment']['file']))
        reader.decrypt(key)
        writer = PdfWriter()
        with open(c.TEMP_PDF, mode='wb+') as temp_pdf:
            for page in reader.pages:
                writer.add_page(page)
            writer.write(temp_pdf)
            # columns = (40, 145, 200, 300, 390, 450, 545, 630, 725, 810, 895, 985, 1065, 1160, 1245, 1335, 1410)
            columns = (40, 145, 200, 320, 390, 450, 545, 630, 725, 810, 895, 985, 1065, 1160, 1245, 1335, 1410)
            areas = [[210.537,16.265,597.275,1429.485], [232.224,19.879,613.54,1425.871]]
            dfs = tabula.read_pdf(temp_pdf, pages=[1, 2], area=areas, multiple_tables=True, columns=columns, lattice=True)
        os.remove(c.TEMP_PDF)
        for i in range(1, 3):
            df_to_process.append(clean_dfs(dfs[i]))
    return merge_name_type(df_to_process)

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

def write_to_csv(df):
    df.to_csv(c.CSV_PATH, index=False, encoding='utf-8')

def main():
    emails_dct = {}
    read_emails = read_emails_id_file(set())
    
    creds = check_credentials(None)
    
    try:
        service = build(c.API_NAME, c.API_VERSION, credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=10, q=query).execute()
        messages = results.get('messages', [])
    except HttpError as error:
        print(f'An error occured: {error}')
        
    if not messages:
        print('No messages found.')
        return
    
    emails_dct = get_messages_details(service, emails_dct, read_emails, messages)
    merged_dfs = read_from_pdf(docs['key'], emails_dct)
    write_to_csv(merged_dfs)
    exit()
    write_emails_id_file(read_emails)
    
    
if __name__ == "__main__":
    main()

    

