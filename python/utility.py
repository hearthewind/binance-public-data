import os, sys, re, shutil, logging, time
from pathlib import Path
from datetime import *
import requests
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from enums import *

# Timeouts: (connect_seconds, read_seconds per chunk)
CONNECT_TIMEOUT = 30
READ_TIMEOUT    = 60
MAX_RETRIES     = 5

_logger = logging.getLogger("binance_downloader")


def setup_logging(log_path=None):
    fmt = "%(asctime)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_path:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format=fmt, datefmt=datefmt, handlers=handlers)


def get_destination_dir(file_url, folder=None):
    store_directory = os.environ.get('STORE_DIRECTORY')
    if folder:
        store_directory = folder
    if not store_directory:
        store_directory = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(store_directory, file_url)


def get_download_url(file_url):
    return "{}{}".format(BASE_URL, file_url)


def get_all_symbols(type):
    if type == 'um':
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    elif type == 'cm':
        url = "https://dapi.binance.com/dapi/v1/exchangeInfo"
    else:
        url = "https://api.binance.com/api/v3/exchangeInfo"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            response.raise_for_status()
            return [s['symbol'] for s in response.json()['symbols']]
        except Exception as e:
            if attempt == MAX_RETRIES:
                _logger.error("Failed to fetch symbols after %d attempts: %s", MAX_RETRIES, e)
                raise
            wait = min(2 ** attempt, 120)
            _logger.warning("Symbol fetch attempt %d/%d failed (%s) — retrying in %ds", attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)


def download_file(base_path, file_name, date_range=None, folder=None):
    download_path = "{}{}".format(base_path, file_name)
    if folder:
        base_path = os.path.join(folder, base_path)
    if date_range:
        date_range = date_range.replace(" ", "_")
        base_path = os.path.join(base_path, date_range)
    save_path = get_destination_dir(os.path.join(base_path, file_name), folder)

    if os.path.exists(save_path):
        _logger.info("SKIP (exists) | %s", file_name)
        return

    if not os.path.exists(base_path):
        Path(get_destination_dir(base_path)).mkdir(parents=True, exist_ok=True)

    download_url = get_download_url(download_path)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _logger.info("DOWNLOADING%s | %s", "" if attempt == 1 else f" (retry {attempt-1}/{MAX_RETRIES-1})", file_name)
            t0 = time.monotonic()

            response = requests.get(download_url, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

            if response.status_code == 404:
                _logger.info("NOT FOUND   | %s", download_url)
                return

            response.raise_for_status()

            blocksize = 8192
            length = response.headers.get('content-length')
            if length:
                length = int(length)
                blocksize = max(4096, length // 100)

            dl_progress = 0
            try:
                with open(save_path, 'wb') as out_file:
                    for buf in response.iter_content(chunk_size=blocksize):
                        if not buf:
                            continue
                        out_file.write(buf)
                        dl_progress += len(buf)
                        if length:
                            done = int(50 * dl_progress / length)
                            sys.stdout.write("\r[%s%s]" % ('#' * done, '.' * (50 - done)))
                            sys.stdout.flush()
            except Exception:
                # Clean up partial file so next run can retry
                if os.path.exists(save_path):
                    os.remove(save_path)
                raise

            elapsed = time.monotonic() - t0
            _logger.info("\nOK          | %s (%.1fs)", file_name, elapsed)
            return

        except requests.exceptions.HTTPError as e:
            # 4xx other than 404 — unlikely to recover, give up
            _logger.error("HTTP ERROR  | %s — %s", file_name, e)
            return

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == MAX_RETRIES:
                _logger.error("GAVE UP     | %s after %d attempts — %s", file_name, MAX_RETRIES, e)
                return
            wait = min(2 ** attempt, 120)
            _logger.warning("RETRY %d/%d  | %s — %s — waiting %ds", attempt, MAX_RETRIES - 1, file_name, e, wait)
            time.sleep(wait)

        except Exception as e:
            _logger.error("ERROR       | %s — %s", file_name, e)
            return


def convert_to_date_object(d):
    year, month, day = [int(x) for x in d.split('-')]
    date_obj = date(year, month, day)
    return date_obj


def get_start_end_date_objects(date_range):
    start, end = date_range.split()
    start_date = convert_to_date_object(start)
    end_date = convert_to_date_object(end)
    return start_date, end_date


def match_date_regex(arg_value, pat=re.compile(r'\d{4}-\d{2}-\d{2}')):
    if not pat.match(arg_value):
        raise ArgumentTypeError
    return arg_value


def check_directory(arg_value):
    # Accept existing directories — we're resuming, not overwriting
    return arg_value


def raise_arg_error(msg):
    raise ArgumentTypeError(msg)


def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
    trading_type_path = 'data/spot'
    if trading_type != 'spot':
        trading_type_path = f'data/futures/{trading_type}'
    if interval is not None:
        path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'
    else:
        path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
    return path


def get_parser(parser_type):
    parser = ArgumentParser(
        description=("This is a script to download historical {} data").format(parser_type),
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument('-s', dest='symbols', nargs='+',
        help='Single symbol or multiple symbols separated by space')
    parser.add_argument('-y', dest='years', default=YEARS, nargs='+', choices=YEARS,
        help='Single year or multiple years separated by space\n-y 2019 2021 means to download {} from 2019 and 2021'.format(parser_type))
    parser.add_argument('-m', dest='months', default=MONTHS, nargs='+', type=int, choices=MONTHS,
        help='Single month or multiple months separated by space\n-m 2 12 means to download {} from feb and dec'.format(parser_type))
    parser.add_argument('-d', dest='dates', nargs='+', type=match_date_regex,
        help='Date to download in [YYYY-MM-DD] format\nsingle date or multiple dates separated by space\ndownload from 2020-01-01 if no argument is parsed')
    parser.add_argument('-startDate', dest='startDate', type=match_date_regex,
        help='Starting date to download in [YYYY-MM-DD] format')
    parser.add_argument('-endDate', dest='endDate', type=match_date_regex,
        help='Ending date to download in [YYYY-MM-DD] format')
    parser.add_argument('-folder', dest='folder', type=check_directory,
        help='Directory to store the downloaded data')
    parser.add_argument('-skip-monthly', dest='skip_monthly', default=0, type=int, choices=[0, 1],
        help='1 to skip downloading of monthly data, default 0')
    parser.add_argument('-skip-daily', dest='skip_daily', default=0, type=int, choices=[0, 1],
        help='1 to skip downloading of daily data, default 0')
    parser.add_argument('-c', dest='checksum', default=0, type=int, choices=[0, 1],
        help='1 to download checksum file, default 0')
    parser.add_argument('-t', dest='type', required=True, choices=TRADING_TYPE,
        help='Valid trading types: {}'.format(TRADING_TYPE))
    parser.add_argument('--symbol-timeout', dest='symbol_timeout', default=600, type=int,
        help='Max seconds to spend on a single symbol before skipping (default: 600)')
    parser.add_argument('--log-file', dest='log_file', default=None,
        help='Path to write structured download log (default: none)')

    if parser_type == 'klines':
        parser.add_argument('-i', dest='intervals', default=INTERVALS, nargs='+', choices=INTERVALS,
            help='single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week')

    return parser
