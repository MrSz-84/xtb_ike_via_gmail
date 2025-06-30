import requests, os, datetime, pandas as pd
from google.cloud import storage
from config import consts as c

#Those three variables are used for testing reasons and one opened from file.
os.environ['NBP_REQ_TYPE'] = 'last'
os.environ['NBP_START_DATE'] = '2025-05-23'
os.environ['NBP_END_DATE'] = '2025-06-29'
with open('./config/xtb-ike-wallet-0a604e129e1a.json', mode='r', encoding='utf-8') as f:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/xtb-ike-wallet-0a604e129e1a.json'

last_full_flag = os.environ['NBP_REQ_TYPE']


def create_requests_url(base_req, type='last'):
    if type == 'last':
        start = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        stop = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        # start = datetime.date.today().strftime('%Y-%m-%d')
        # stop = datetime.date.today().strftime('%Y-%m-%d')
        mid = f'{base_req}{c.NBP_MID}/usd/{start}/{stop}/?format=json'.lower()
        ask_bid = f'{base_req}{c.NBP_ASK_BID}/usd/{start}/{stop}/?format=json'.lower()
        return mid, ask_bid
    else:
        start = os.environ['NBP_START_DATE']
        stop = os.environ['NBP_END_DATE']
        mid = f'{base_req}{c.NBP_MID}/usd/{start}/{stop}/?format=json'.lower()
        ask_bid = f'{base_req}{c.NBP_ASK_BID}/usd/{start}/{stop}/?format=json'.lower()
        return mid, ask_bid

def get_data(pair):
    temp_data = []
    count = 1
    for req in pair:
        r = requests.get(req)
        if r.status_code == 200:
            count += 1
            temp_data.append(r.json())

        elif r.status_code == 404:
            if count == 1:
                print(f'API call for mid raiting returned {r.status_code}.')
            else:
                print(f'API call for ask & bid raitings returned {r.status_code}.')
            print(f'Response text: {r.status_code} {r.reason}')
            return [{'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}, {'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}]
        elif r.status_code == 400:
            if count == 1:
                print(f'API call for mid raiting returned {r.status_code}.')
            else:
                print(f'API call for ask % bid raitings returned {r.status_code}.')
            print(f'Response text: {r.status_code} {r.reason}')
            return [{'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}, {'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}]
    return temp_data

def create_data_struct(input_data):
    output_lst = []
    currency = input_data[0]['code']
    for items in zip(input_data[0]['rates'], input_data[1]['rates']):
        dct = {}
        dct['date'] = items[0]['effectiveDate']
        dct['currency'] = currency
        dct['mid'] = items[0]['mid']
        dct['ask'] = items[1]['ask']
        dct['bid'] = items[1]['bid']
        output_lst.append(dct)
    return output_lst

def create_csv(to_write):
    header = 'date,currency,mid,ask,bid\n'
    with open(c.NBP_TMP_CSV, mode='w+') as f:
        f.write(header)
        for entry in to_write:
            line = ''
            line += f'{entry['date']},'
            line += f'{entry['currency']},'
            line += f'{entry['mid']},'
            line += f'{entry['ask']},'
            line += f'{entry['bid']}\n'
            f.write(line)

def upload_to_bucket(fname, gsbucket, dest_fname):
    dest_fname = dest_fname.replace('./tmp/','')
    client = storage.Client()
    bucket = client.bucket(gsbucket)
    blob = bucket.blob(dest_fname)
    blob.upload_from_filename(fname)
    print(f'✅ Upload of the file {fname} to {gsbucket} cloud storage bucket complete.')

def main():
    req_pair = create_requests_url(base_req=c.NBP_BASE_REQ, type=last_full_flag)
    input_ = get_data(req_pair)
    if input_[0]['code'] == 'empty' and input_[1]['code'] == 'empty':
        print(f'❌ Mid table response status: {input_[0]['rates']}')
        print(f'❌ Ask|Bid table response status: {input_[1]['rates']}')
        return
    output = create_data_struct(input_)
    create_csv(output)
    
    try:
        upload_to_bucket(c.NBP_TMP_CSV, c.NBP_BUCKET_PATH, c.NBP_TMP_CSV)
    except Exception as e:
        os.remove(c.NBP_TMP_CSV)
        print(f'❌ An error occured during upload to bucket: {e}')
    os.remove(c.NBP_TMP_CSV)
    
    print('✅ All tasks done, finishing program...')
    #TODO USE ARGPARSE FOR FLAG INPUT FOR DATE RANGE. CANGE THE TODAY - 1 DAY TO START AND END DATE FROM FLAGS

if __name__ == "__main__":
    main()
