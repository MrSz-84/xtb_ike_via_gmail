import requests, os, datetime, argparse, json
from google.cloud import storage
from config import consts as c

with open('./config/xtb-ike-wallet-0a604e129e1a.json', mode='r', encoding='utf-8') as f:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/xtb-ike-wallet-0a604e129e1a.json'
with open(c.ALPHA_API, mode='r', encoding='utf-8') as f:
    os.environ['ALPHA_API_KEY'] = json.load(f).strip('"')
    



# def validate_dates(dates):
#     start = datetime.datetime.strptime(dates[0], '%Y-%m-%d')
#     stop = datetime.datetime.strptime(dates[1], '%Y-%m-%d')
#     if start <= stop:
#         return dates
#     else:
#         return [stop.strftime('%Y-%m-%d'), start.strftime('%Y-%m-%d')]

# def argparse_logic():
#     start = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#     stop = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

#     parser = argparse.ArgumentParser(
#         prog='NBP USD_PLN currency exchange rate fetcher',
#         description='''Fetch currency exchange rates data from NBP\'s API for for last day,
#         or given date period (up to 93 days). Use flags -l for last day or -p for givben period.
#         Type -h for help''',
#         epilog='For more information wtite at pokeplacek@gmail.com.'
#     )
#     group = parser.add_mutually_exclusive_group()
#     group.add_argument(
#         '-l',
#         '--last',
#         action='store_true',
#         help='Fetch currency exchange rates for previous day.'
#     )
#     group.add_argument(
#         '-p',
#         '--period',
#         action='store_true',
#         help='Fetch currency exchange rates for given period up to 93 days'
#     )
#     parser.add_argument(
#         '-d',
#         '--dates',
#         metavar='D',
#         nargs=2,
#         default=[start, stop],
#         type=str,
#         help='Dates in ISO format for --period option. Max period length 93 days.'
#     )
#     args = parser.parse_args()
#     if not args.last and not args.period:
#         args.last = True
#     args.dates = validate_dates(args.dates)
#     if args.last:
#         os.environ['NBP_REQ_TYPE'] = 'last'
#     else:
#         os.environ['NBP_REQ_TYPE'] = 'period'
#     os.environ['NBP_START_DATE'] = args.dates[0]
#     os.environ['NBP_END_DATE'] = args.dates[1]

# def create_requests_url(base_req, type='last'):
#     if type == 'last':
#         start = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#         stop = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#         # start = datetime.date.today().strftime('%Y-%m-%d')
#         # stop = datetime.date.today().strftime('%Y-%m-%d')
#         mid = f'{base_req}{c.NBP_MID}/usd/{start}/{stop}/?format=json'.lower()
#         ask_bid = f'{base_req}{c.NBP_ASK_BID}/usd/{start}/{stop}/?format=json'.lower()
#         return mid, ask_bid
#     else:
#         start = os.environ['NBP_START_DATE']
#         stop = os.environ['NBP_END_DATE']
#         mid = f'{base_req}{c.NBP_MID}/usd/{start}/{stop}/?format=json'.lower()
#         ask_bid = f'{base_req}{c.NBP_ASK_BID}/usd/{start}/{stop}/?format=json'.lower()
#         return mid, ask_bid

# def get_data(pair):
#     temp_data = []
#     count = 1
#     for req in pair:
#         r = requests.get(req)
#         if r.status_code == 200:
#             count += 1
#             temp_data.append(r.json())
            
#         elif r.status_code == 404:
#             if count == 1:
#                 print(f'API call for mid raiting returned {r.status_code}.')
#             else:
#                 print(f'API call for ask & bid raitings returned {r.status_code}.')
#             print(f'Response text: {r.status_code} {r.reason}')
#             return [{'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}, {'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}]
#         elif r.status_code == 400:
#             if count == 1:
#                 print(f'API call for mid raiting returned {r.status_code}.')
#             else:
#                 print(f'API call for ask % bid raitings returned {r.status_code}.')
#             print(f'Response text: {r.status_code} {r.reason}')
#             return [{'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}, {'code': 'empty', 'rates': f'{r.status_code} {r.reason}'}]
#     return temp_data

# def create_data_struct(input_data):
#     output_lst = []
#     currency = input_data[0]['code']
#     for items in zip(input_data[0]['rates'], input_data[1]['rates']):
#         dct = {}
#         dct['date'] = items[0]['effectiveDate']
#         dct['currency'] = currency
#         dct['mid'] = items[0]['mid']
#         dct['ask'] = items[1]['ask']
#         dct['bid'] = items[1]['bid']
#         output_lst.append(dct)
#     return output_lst

# def create_csv(to_write):
#     header = 'date,currency,mid,ask,bid\n'
#     with open(c.NBP_TMP_CSV, mode='w+') as f:
#         f.write(header)
#         for entry in to_write:
#             line = ''
#             line += f'{entry['date']},'
#             line += f'{entry['currency']},'
#             line += f'{entry['mid']},'
#             line += f'{entry['ask']},'
#             line += f'{entry['bid']}\n'
#             f.write(line)

# def upload_to_bucket(fname, gsbucket, dest_fname):
#     dest_fname = dest_fname.replace('./tmp/','')
#     client = storage.Client()
#     bucket = client.bucket(gsbucket)
#     blob = bucket.blob(dest_fname)
#     blob.upload_from_filename(fname)
#     print(f'✅ Upload of the file {fname} to {gsbucket} cloud storage bucket complete.')

def main():
    print(os.environ['ALPHA_API_KEY'])
    
    
    # argparse_logic()
    # req_pair = create_requests_url(base_req=c.NBP_BASE_REQ, type=os.environ['NBP_REQ_TYPE'])
    # input_ = get_data(req_pair)
    # if input_[0]['code'] == 'empty' and input_[1]['code'] == 'empty':
    #     print(f'❌ Mid table response status: {input_[0]['rates']}')
    #     print(f'❌ Ask|Bid table response status: {input_[1]['rates']}')
    #     return
    # output = create_data_struct(input_)
    # create_csv(output)
    
    # try:
    #     upload_to_bucket(c.NBP_TMP_CSV, c.NBP_BUCKET_PATH, c.NBP_TMP_CSV)
    # except Exception as e:
    #     os.remove(c.NBP_TMP_CSV)
    #     print(f'❌ An error occured during upload to bucket: {e}')
    #     return
    # os.remove(c.NBP_TMP_CSV)
    
    # print('✅ All tasks done, finishing program...') 

if __name__ == "__main__":
    main()
