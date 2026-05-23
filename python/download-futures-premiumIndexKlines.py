#!/usr/bin/env python

"""
  script to download premiumIndexKlines.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-futures-premiumIndexKlines.py

  Robustness features:
  - connect + read timeouts on every HTTP request (no more indefinite hangs)
  - exponential-backoff retry on network errors (up to 5 attempts)
  - per-symbol watchdog: symbol is skipped if it takes longer than --symbol-timeout seconds
  - partial files deleted on failure so next run can retry cleanly
  - structured log file via --log-file
"""

import sys
import logging
import concurrent.futures
from datetime import *
import pandas as pd
from enums import START_DATE, END_DATE, DAILY_INTERVALS, PERIOD_START_DATE
from utility import (
    download_file, get_all_symbols, get_parser, setup_logging,
    convert_to_date_object, get_path, raise_arg_error
)

_logger = logging.getLogger("binance_downloader")


def _download_symbol_monthly(trading_type, symbol, intervals, years, months, start_date, end_date, date_range, folder, checksum):
    for interval in intervals:
        for year in years:
            for month in months:
                current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                if start_date <= current_date <= end_date:
                    path = get_path(trading_type, "premiumIndexKlines", "monthly", symbol, interval)
                    file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                    download_file(path, file_name, date_range, folder)

                    if checksum == 1:
                        checksum_path = get_path(trading_type, "premiumIndexKlines", "monthly", symbol, interval)
                        checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                        download_file(checksum_path, checksum_file_name, date_range, folder)


def _download_symbol_daily(trading_type, symbol, intervals, dates, start_date, end_date, date_range, folder, checksum):
    for interval in intervals:
        for date in dates:
            current_date = convert_to_date_object(date)
            if start_date <= current_date <= end_date:
                path = get_path(trading_type, "premiumIndexKlines", "daily", symbol, interval)
                file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
                download_file(path, file_name, date_range, folder)

                if checksum == 1:
                    checksum_path = get_path(trading_type, "premiumIndexKlines", "daily", symbol, interval)
                    checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
                    download_file(checksum_path, checksum_file_name, date_range, folder)


def download_monthly_premiumIndexKlines(trading_type, symbols, num_symbols, intervals, years, months,
                                         start_date, end_date, folder, checksum, symbol_timeout):
    date_range = None
    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    _logger.info("Starting monthly premiumIndexKlines — %d symbols", num_symbols)

    for idx, symbol in enumerate(symbols, 1):
        _logger.info("[%d/%d] Monthly premiumIndexKlines: %s", idx, num_symbols, symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _download_symbol_monthly,
                trading_type, symbol, intervals, years, months,
                start_date, end_date, date_range, folder, checksum
            )
            try:
                future.result(timeout=symbol_timeout)
            except concurrent.futures.TimeoutError:
                _logger.warning("TIMEOUT     | %s exceeded %ds — skipping to next symbol", symbol, symbol_timeout)
            except Exception as e:
                _logger.error("ERROR       | %s — %s", symbol, e)


def download_daily_premiumIndexKlines(trading_type, symbols, num_symbols, intervals, dates,
                                       start_date, end_date, folder, checksum, symbol_timeout):
    date_range = None
    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    _logger.info("Starting daily premiumIndexKlines — %d symbols", num_symbols)

    for idx, symbol in enumerate(symbols, 1):
        _logger.info("[%d/%d] Daily premiumIndexKlines: %s", idx, num_symbols, symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _download_symbol_daily,
                trading_type, symbol, intervals, dates,
                start_date, end_date, date_range, folder, checksum
            )
            try:
                future.result(timeout=symbol_timeout)
            except concurrent.futures.TimeoutError:
                _logger.warning("TIMEOUT     | %s exceeded %ds — skipping to next symbol", symbol, symbol_timeout)
            except Exception as e:
                _logger.error("ERROR       | %s — %s", symbol, e)


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    setup_logging(log_path=args.log_file)

    if args.type == 'spot':
        raise_arg_error('Valid Type: um, cm')

    if not args.symbols:
        _logger.info("Fetching all symbols from exchange...")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)

    _logger.info("Found %d symbols", num_symbols)

    if args.dates:
        dates = args.dates
    else:
        period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(PERIOD_START_DATE)
        dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
        dates = [date.strftime("%Y-%m-%d") for date in dates]
        if args.skip_monthly == 0:
            download_monthly_premiumIndexKlines(
                args.type, symbols, num_symbols, args.intervals, args.years, args.months,
                args.startDate, args.endDate, args.folder, args.checksum,
                args.symbol_timeout
            )

    if args.skip_daily == 0:
        download_daily_premiumIndexKlines(
            args.type, symbols, num_symbols, args.intervals, dates,
            args.startDate, args.endDate, args.folder, args.checksum,
            args.symbol_timeout
        )
