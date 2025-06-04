import os
import re
import pytz
import base64


from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import consts as c


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def parse_date(raw):
    dt = datetime.strptime(raw, '%a, %d %b %Y %H:%M:%S %z')
    local_tz = pytz.timezone('Europe/Warsaw')
    local_dt = dt.astimezone(local_tz)
    iso_date = local_dt.isoformat()
    return iso_date

def parse_senred(to_parse):
    email = re.search(r'<(.*)>', to_parse).group(1)
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


def main():
    emails_dct = {}
    read_emails = read_emails_id_file(set())
    
    creds = None
    
    if os.path.exists(c.TOKEN):
        creds = Credentials.from_authorized_user_file(c.TOKEN, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                c.SECRET, SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(c.TOKEN, 'w') as token:
            token.write(creds.to_json())
    
    query = f'from:{c.SENDER} subject:{c.SUBJECT} has:attachment filename:{c.SPEC_ATTACHMENT} older_than:{c.OLDER_THAN} newer_than:{c.NEWER_THAN}'
                
    try:
        service = build(c.API_NAME, c.API_VERSION, credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=10, q=query).execute()
        messages = results.get('messages', [])
    except HttpError as error:
        print(f'An error occured: {error}')
        
    if not messages:
        print('No messages found.')
        return
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
        msg_id = msg['id']
        headers = msg['payload']['headers']
        
        sender = next((header['value'] for header in headers if header['name'] == 'From'), 'Brak nadawcy')
        sender = parse_senred(sender)
        to = next((header['value'] for header in headers if header['name'] == 'To'), 'Brak odbiorcy')
        subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'Brak tematu')
        date = next((header['value'] for header in headers if header['name'] == 'Date'), 'Brak daty')
        date = parse_date(date)
        
        attachments = []
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
                        attachments.append(file_name)
                        attachments.append(file_bytes)
        if msg_id not in read_emails:
            read_emails.add(msg_id)
            emails_dct[msg_id] =  {
                'date': date, 
                'from': sender, 
                'to': to, 
                'subject': subject,
                'attachment': {'name': file_name, 'file': file_bytes} #file_name
            }
        
    write_emails_id_file(read_emails)
        
if __name__ == "__main__":
    main()

