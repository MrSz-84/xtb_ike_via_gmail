from simplegmail import Gmail
from simplegmail.query import construct_query
from config import consts as c
import json

with open('./config/docs.json', mode='r', encoding='utf-8') as creds:
    docs = json.load(creds)
gmail = Gmail(client_secret_file='./config/api_oauth.json', creds_file='./config/gmail_token.json')

params = {
    'sender': c.SENDER,
    'subject': c.SUBJECT,
    'attachment': c.ATTACHMENT,
    'spec_attachment': c.SPEC_ATTACHMENT,
    'older_than': c.OLDER_THAN,
    'newer_than': c.NEWER_THAN
}

messages = gmail.get_messages(query=construct_query(params))
for message in messages:
    print('To: ', message.recipient)
    print('From: ', message.sender)
    print('Subject: ', message.subject)
    print('When: ', message.date)
    print('Snippet: ', message.snippet)
    print()
    
quit()
    