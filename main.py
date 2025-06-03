import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import consts as c


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    
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
    
    query = f'from:{c.SENDER} subject:{c.SUBJECT} has:attachment filename:{c.SPEC_ATTACHMENT} older_than:1d newer_than:1m'
                
    try:
        service = build(c.API_NAME, c.API_VERSION, credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=10, q=query).execute()
        messages = results.get('messages', [])
        
        if not messages:
            print('No messages found.')
            return
        print('Message:')
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            
            headers = msg['payload']['headers']
            
            sender = next((header['value'] for header in headers if header['name'] == 'From'), 'Brak nadawcy')
            to = next((header['value'] for header in headers if header['name'] == 'To'), 'Brak odbiorcy')
            subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'Brak tematu')
            date = next((header['value'] for header in headers if header['name'] == 'Date'), 'Brak daty')
            
            attachments = 'Brak załączników'
            if 'parts' in msg['payload']:
                attachments = [part['filename'] for part in msg['payload']['parts']]
                attachments = attachments if attachments else 'Brak załączników'
            
            snippet = msg.get('snippet', 'Brak podglądu')
            print(f'ID: {msg['id']}')
            print(f'Data: {date}')
            print(f'Nadawca: {sender}')
            print(f'Odbiorca: {to}')
            print(f'Temat: {subject}')
            print(f'Załączniki: {attachments}')
            print(f'Podgląd: {snippet}')
            print('-' * 80)

            
            
    except HttpError as error:
        print(f'An error occured: {error}')
        
if __name__ == "__main__":
    main()