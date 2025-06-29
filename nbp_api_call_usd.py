import requests, os
from config import consts as c

#Those three variables are used for testing reasons.
os.environ['NBP_REQ_TYPE'] = 'lastaaa'
os.environ['NBP_START_DATE'] = '2025-05-23'
os.environ['NBP_END_DATE'] = '2025-06-28'

last_full_flag = os.environ['NBP_REQ_TYPE']


def create_requests_url(base_req, type='last'):
    if type == 'last':
        mid = f'{base_req}{c.NBP_MID}/usd/last/1/?format=json'.lower()
        ask_bid = f'{base_req}{c.NBP_ASK_BID}/usd/last/1/?format=json'.lower()
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
                print(f'API call for ask % bid raitings returned {r.status_code}.')
            print('Nothing found for given period, check what you\' looking for.')
            r.raise_for_status()
        elif r.status_code == 400:
            if count == 1:
                print(f'API call for mid raiting returned {r.status_code}.')
            else:
                print(f'API call for ask % bid raitings returned {r.status_code}.')
            print('Bad request parameters or too many data points asked. Check limits and/or the request')
            r.raise_for_status()
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

def main():
    req_pair = create_requests_url(base_req=c.NBP_BASE_REQ, type=last_full_flag)
    input_ = get_data(req_pair)
    output = create_data_struct(input_)
    create_csv(output)
    #TODO CREATE UPLOAD TO BUCKET FUNCTION
    #TODO USE ARGPARSE FOR FLAG INPUT FOR DATE RANGE. CANGE THE TODAY - 1 DAY TO START AND END DATE FROM FLAGS

if __name__ == "__main__":
    main()
