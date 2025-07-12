import requests, os, datetime, argparse, json, time, asyncio
from google.cloud import storage
from config import consts as c

with open('./config/xtb-ike-wallet-0a604e129e1a.json', mode='r', encoding='utf-8') as f:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/xtb-ike-wallet-0a604e129e1a.json'
with open(c.ALPHA_API, mode='r', encoding='utf-8') as f:
    os.environ['ALPHA_API_KEY'] = json.load(f).strip('"')
    

# TODO async version of the code - json parsing and ttl async corutine. Slower than synchroneous version. Tests needed for real API calls instead of reading from SSD.
# TODO make temp files cleaning function. and pass it every time something needs to be removed.
# TODO Make a mechanism for downloading min_max_dates from cloud storage, and also a mechanism for uploading it to the cloud
# TODO Make a mechanism for uploading equity and fx csv to the cloud

def read_json(path):
    with open(path, mode='r', encoding='utf-8') as f:
        contents =  f.read()
        return json.loads(contents)

def parse_fx(api_res, min_max):
    from_symbol = api_res['Meta Data']['2. From Symbol']
    to_symbol = api_res['Meta Data']['3. To Symbol']
    pair = from_symbol + to_symbol
    values = 'Time Series FX (Daily)'
    meta = {'from': from_symbol, 'to': to_symbol, 'symbol': pair}
    return _parse_frame(api_res[values], meta, min_max)

def parse_equity(api_res, min_max):
    symbol = api_res['Meta Data']['2. Symbol'].replace('.LON', '.UK')
    values = 'Time Series (Daily)'
    return _parse_frame(api_res[values], {'symbol': symbol}, min_max)

def _parse_frame(time_series, metadata, min_max):
    dct = {}
    for k, v in time_series.items():
        tmp_dct = {vk.split(' ')[1]: vv for vk, vv in v.items()}
        tmp_dct.update(metadata)
        if k >= min_max[tmp_dct['symbol']]['min_date'] and k <= min_max[tmp_dct['symbol']]['max_date']:
            continue
        else:
            dct[k] = tmp_dct
    return dct

def parse_all_api_res(api_res, data_type, min_max):
    if data_type == 'fx':
        return parse_fx(api_res, min_max)
    else:
        return parse_equity(api_res, min_max)

def create_csv(batch: list[dict[dict]]):
    new_min_max = {}
    ## TODO delete those two checks when proper logic in main() will be done
    if os.path.exists(c.ALPHA_EQUITY_CSV):
        os.remove(c.ALPHA_EQUITY_CSV)
    if os.path.exists(c.ALPHA_FX_CSV):
        os.remove(c.ALPHA_FX_CSV)
    header_equity = 'date,open,high,low,close,volume,symbol\n'
    header_fx = 'date,open,high,low,close,from,to,symbol\n'
    for i, equity in enumerate(batch):
        if i < len(batch) - 1:
            with open(c.ALPHA_EQUITY_CSV, mode='a', encoding='utf-8') as f:
                if i == 0:
                    f.write(header_equity)
                for day, entry in equity.items():
                    line = f'{day},' + ','.join(str(v) for v in entry.values()) + '\n'
                    f.write(line)
            new_min_max[list(equity.values())[0]['symbol']] = {'min_date': min(equity.keys()), 'max_date': max(equity.keys())}
        else:
            with open(c.ALPHA_FX_CSV, mode='a', encoding='utf-8') as f:
                f.write(header_fx)
                for day, entry in equity.items():
                    line = f'{day},' + ','.join(str(v) for v in entry.values()) +'\n'
                    f.write(line)
            new_min_max[list(equity.values())[0]['symbol']] = {'min_date': min(equity.keys()), 'max_date': max(equity.keys())}
    return new_min_max

def read_min_max(path):
    with open(path, mode='r', encoding='utf-8') as f:
        header = f.readline().strip().split(',')
        min_max = {
            line[0]: {k: v for k, v in zip(header[1:], line[1:])} 
            for line in (l.strip().split(',') for l in f)
            }
    return min_max

def min_max_compare(old_min_max, new_min_max):
    swapped_min_max = old_min_max.copy()
    symbols = [s for s in old_min_max]
    for symbol in symbols:
        if new_min_max[symbol]['min_date'] < old_min_max[symbol]['min_date']:
            swapped_min_max[symbol]['min_date'] = new_min_max[symbol]['min_date']
        if new_min_max[symbol]['max_date'] > old_min_max[symbol]['max_date']:
            swapped_min_max[symbol]['max_date'] = new_min_max[symbol]['max_date']
    return write_min_max(swapped_min_max)

def write_min_max(data):
    print(data)
    header = 'symbo,min_date,max_date\n'
    with open(c.ALPHA_MIN_MAX, mode='w', encoding='utf-8') as f:
        f.write(header)
        for symbol, dates in data.items():
            line = f'{symbol},' + ','.join(date for date in dates.values()) + '\n'
            f.write(line)

def is_parsed_empty(parsed):
    is_empty = []
    for elem in parsed:
        if len(elem) == 0:
            is_empty.append(True)
        else:
            is_empty.append(False)
    return any(is_empty)

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
    start = time.time()
    min_max = read_min_max(c.ALPHA_MIN_MAX)
    files = ['./tmp/eimi_api.json', './tmp/igln_api.json', './tmp/iwda_api.json', './tmp/fx_usdpln_api.json']
    tasks = [read_json(file) for file in files]
    # results = tasks
    
    
    # for i, res in enumerate(tasks):
    parsed = [parse_all_api_res(res, data_type='etf' if i < 3 else 'fx', min_max=min_max) for i, res in enumerate(tasks)]
    if is_parsed_empty(parsed):
        print('empty')
        print('make some cleaning')
        return

        
    new_min_max = create_csv(parsed)
    old_min_max = read_min_max(c.ALPHA_MIN_MAX)
    # min_max_compare(old_min_max, new_min_max)
    
    
    stop = time.time()
    print('Duration: ', stop - start)
    
    
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
