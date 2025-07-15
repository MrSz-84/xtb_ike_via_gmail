import requests, os, copy, datetime, argparse, json, time, asyncio, aiohttp
from google.cloud import storage
from config import consts as c

with open('./config/xtb-ike-wallet-0a604e129e1a.json', mode='r', encoding='utf-8') as f:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './config/xtb-ike-wallet-0a604e129e1a.json'
with open(c.ALPHA_API, mode='r', encoding='utf-8') as f:
    os.environ['ALPHA_API_KEY'] = json.load(f).strip('"')
# os.environ['OUTPUT_SIZE_TYPE'] = c.ALPHA_OUTPUT_SIZE_TYPE


def argparse_logic():

    parser = argparse.ArgumentParser(
        prog='Alphavantage API Equity and FX data fetcher.',
        description='''Not much to add hete. There are two options for data output size, compact and full. 
        Try to use compact which returns 100 datapoints, because full returns 20+ years of daily data in some cases...''',
        epilog='For more information wtite at pokeplacek@gmail.com.'
    )
    parser.add_argument(
        '-os',
        '--outputsize',
        metavar='OS',
        choices=['compact', 'full', 'test'],
        default='compact',
        type=str,
        help='100 days vs 20+ years od data, choose compact :D'
    )
    args = parser.parse_args()
    os.environ['OUTPUT_SIZE_TYPE'] = args.outputsize

def get_symbols(responses):
    eq = 0
    fx = 0
    counter = 0
    for response in responses:
        counter += 1
        if response['Meta Data']['1. Information'].startswith('Forex'):
            fx += 1
            pair = response['Meta Data']['2. From Symbol'] + response['Meta Data']['3. To Symbol']
            c.ALPHA_TICKER_SYMBOLS.append(pair)
        else:
            eq += 1
            symbol = response['Meta Data']['2. Symbol'].replace('.LON', '.UK')
            c.ALPHA_TICKER_SYMBOLS.append(symbol)
    c.ALPHA_EQ = eq
    c.ALPHA_FX = fx

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

def files_cleanup(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)

def create_csv(batch: list[dict[dict]]):
    new_min_max = {}
    files_cleanup((c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV))
    header_equity = 'date,open,high,low,close,volume,symbol\n'
    header_fx = 'date,open,high,low,close,from,to,symbol\n'
    for i, equity in enumerate(batch):
        if i < len(batch) - c.ALPHA_FX:
            with open(c.ALPHA_EQUITY_CSV, mode='a', encoding='utf-8') as f:
                if i == 0:
                    f.write(header_equity)
                for day, entry in equity.items():
                    line = f'{day},' + ','.join(str(v) for v in entry.values()) + '\n'
                    f.write(line)
            new_min_max[list(equity.values())[0]['symbol']] = {'min_date': min(equity.keys()), 'max_date': max(equity.keys())}
        else:
            with open(c.ALPHA_FX_CSV, mode='a', encoding='utf-8') as f:
                if i == len(batch) - c.ALPHA_FX:
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
    swapped_min_max = copy.deepcopy(old_min_max)
    symbols = [s for s in old_min_max]
    for symbol in symbols:
        if new_min_max[symbol]['min_date'] < old_min_max[symbol]['min_date']:
            swapped_min_max[symbol]['min_date'] = new_min_max[symbol]['min_date']
        if new_min_max[symbol]['max_date'] > old_min_max[symbol]['max_date']:
            swapped_min_max[symbol]['max_date'] = new_min_max[symbol]['max_date']
    return write_min_max(swapped_min_max)

def write_min_max(data):
    header = 'symbol,min_date,max_date\n'
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

def create_boilerplate_min_max_file(symbols):
    dates = {'min_date': '2025-01-01', 'max_date': '2025-01-31'}
    header = 'symbol,min_date,max_date\n'
    with open(c.ALPHA_MIN_MAX, mode='w', encoding='utf-8') as f:
        f.write(header)
        for symbol in symbols:
            line = f'{symbol},{dates['min_date']},{dates['max_date']}\n'
            f.write(line)

def upload_to_bucket(fname, gsbucket, dest_fname):
    dest_fname = dest_fname.replace('./tmp/','')
    client = storage.Client()
    bucket = client.bucket(gsbucket)
    blob = bucket.blob(dest_fname)
    blob.upload_from_filename(fname)
    print(f'⬆️ Upload of the file {fname} to {gsbucket} cloud storage bucket complete.')

def download_from_bucket(source_fname, gsbucket, dest_fname):
    source_fname = dest_fname.replace('./tmp/','')
    client = storage.Client()
    bucket = client.bucket(gsbucket)
    blob = bucket.blob(source_fname)
    if blob.exists():
        print(f'⬇️ Cloud Storage file object found. Downloading and writing...  {dest_fname}')
        blob.download_to_filename(dest_fname)
    else:
        print(f'ℹ️ Cloud Storage file object not found. Creating empty file...  {dest_fname}')
        create_boilerplate_min_max_file(c.ALPHA_TICKER_SYMBOLS)

def create_fx_url(pair, base_req):
    from_s = pair[:3]
    to_s = pair[3:]
    r = base_req + c.ALPHA_FX_REQ_FROM_SYMBOL + from_s + c.ALPHA_FX_REQ_TO_SYMBOL \
        + to_s + c.ALPHA_OUTPUT_SIZE + os.environ['OUTPUT_SIZE_TYPE'] \
        + c.ALPHA_APIKEY_REQ + os.environ['ALPHA_API_KEY']
    return r

def create_eq_url(ticker, base_req):
    r = base_req + c.ALPHA_EQ_SYMBOL_REQ + ticker \
        + c.ALPHA_OUTPUT_SIZE + os.environ['OUTPUT_SIZE_TYPE'] \
        + c.ALPHA_APIKEY_REQ + os.environ['ALPHA_API_KEY']
    return r

def create_requests_lst(base_req, symbols):
    requests_lst = []
    for symbol, stype in symbols.items():
        if stype == 'fx' and len(symbol) == 6:
            requests_lst.append(create_fx_url(symbol, base_req[stype]))
        else:
            requests_lst.append(create_eq_url(symbol, base_req[stype]))
    return requests_lst

async def get_data_async(sess, url):
    async with sess.get(url) as r:
        rtext = await r.text()
        limit = 'to instantly remove all daily rate limits'
        if r.status == 200 and limit not in rtext:
            return await r.json()
        elif r.status == 200 and limit in rtext:
            raise Exception(f'Max daily limit of API calls reached.')
        elif r.status == 400:
            print(f'API call for mid raiting returned {r.status_code}.')
            exit()

def upload_min_max():
    try:
        upload_to_bucket(c.ALPHA_MIN_MAX, c.ALPHA_MIN_MAX_BUCKET, c.ALPHA_MIN_MAX)
    except Exception as e:
        files_cleanup([c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV, c.ALPHA_MIN_MAX])
        print(f'❌ An error occured during upload file {c.ALPHA_MIN_MAX} to bucket: {e}')

def upload_equity():
    try:
        upload_to_bucket(c.ALPHA_EQUITY_CSV, c.ALPHA_EQ_BUCKET, c.ALPHA_EQUITY_CSV)
    except Exception as e:
        files_cleanup([c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV, c.ALPHA_MIN_MAX])
        print(f'❌ An error occured during upload file {c.ALPHA_EQUITY_CSV} to bucket: {e}')
        upload_min_max()

def upload_fx():
    try:
        upload_to_bucket(c.ALPHA_FX_CSV, c.ALPHA_FX_BUCKET, c.ALPHA_FX_CSV)
    except Exception as e:
        files_cleanup([c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV, c.ALPHA_MIN_MAX])
        print(f'❌ An error occured during upload file {c.ALPHA_FX_CSV} to bucket: {e}')
        upload_min_max()

async def main():
    start = time.time()
    argparse_logic()
    requests_lst = create_requests_lst(c.ALPHA_REQ_TYPE, c.ALPHA_REQ_SYMBOLS)
    async with aiohttp.ClientSession() as session:
        tasks = [get_data_async(session, req) for req in requests_lst]
        try:
            responses = await asyncio.gather(*tasks)
        except Exception as e:
            print(f'❌ An error occured during API call: {e}')
            return
    # files = ['./tmp/eimi_api.json', './tmp/igln_api.json', './tmp/iwda_api.json', './tmp/fx_usdpln_api.json', './tmp/fx_eurpln_api.json']
    # tasks = [read_json(file) for file in files]
    get_symbols(responses)
    # TODO Uncomment download from bucket, and delete create_boilerplate...()
    download_from_bucket(c.ALPHA_MIN_MAX, c.ALPHA_MIN_MAX_BUCKET, c.ALPHA_MIN_MAX)
    # create_boilerplate_min_max_file(c.ALPHA_TICKER_SYMBOLS)
    
    min_max = read_min_max(c.ALPHA_MIN_MAX)
    parsed = [parse_all_api_res(res, data_type='etf' if i < c.ALPHA_EQ else 'fx', min_max=min_max) for i, res in enumerate(responses)]
    if is_parsed_empty(parsed):
        print(f'ℹ️ No new data was found after parsing API calls. Cleaning up and shutting down...')
        files_cleanup([c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV, c.ALPHA_MIN_MAX])
        return
    
    new_min_max = create_csv(parsed)
    old_min_max = read_min_max(c.ALPHA_MIN_MAX)
    min_max_compare(old_min_max, new_min_max)
    
    if 'equity' in c.ALPHA_REQ_SYMBOLS.values():
        upload_equity()
        print('in equity')
    if 'fx' in c.ALPHA_REQ_SYMBOLS.values():
        upload_fx()
        print('in fx')
    upload_min_max()
    
    stop = time.time()
    print('Duration: ', stop - start)
    
    files_cleanup([c.ALPHA_EQUITY_CSV, c.ALPHA_FX_CSV, c.ALPHA_MIN_MAX])
    print('✅ All tasks done, finishing program...') 

if __name__ == "__main__":
    asyncio.run(main())
