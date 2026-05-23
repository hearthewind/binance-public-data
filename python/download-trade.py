#!/usr/bin/env python

"""
  script to download trades.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-trade.py

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
from enums import *
from utility import (
    download_file, get_all_symbols, get_parser, setup_logging,
    get_start_end_date_objects, convert_to_date_object, get_path
)

_logger = logging.getLogger("binance_downloader")


def _download_symbol_monthly(trading_type, symbol, years, months, start_date, end_date, date_range, folder, checksum):
    for year in years:
        for month in months:
            current_date = convert_to_date_object('{}-{}-01'.format(year, month))
            if current_date >= start_date and current_date <= end_date:
                path = get_path(trading_type, "trades", "monthly", symbol)
                file_name = "{}-trades-{}-{}.zip".format(symbol.upper(), year, '{:02d}'.format(month))
                download_file(path, file_name, date_range, folder)

                if checksum == 1:
                    checksum_path = get_path(trading_type, "trades", "monthly", symbol)
                    checksum_file_name = "{}-trades-{}-{}.zip.CHECKSUM".format(symbol.upper(), year, '{:02d}'.format(month))
                    download_file(checksum_path, checksum_file_name, date_range, folder)


def _download_symbol_daily(trading_type, symbol, dates, start_date, end_date, date_range, folder, checksum):
    for date in dates:
        current_date = convert_to_date_object(date)
        if current_date >= start_date and current_date <= end_date:
            path = get_path(trading_type, "trades", "daily", symbol)
            file_name = "{}-trades-{}.zip".format(symbol.upper(), date)
            download_file(path, file_name, date_range, folder)

            if checksum == 1:
                checksum_path = get_path(trading_type, "trades", "daily", symbol)
                checksum_file_name = "{}-trades-{}.zip.CHECKSUM".format(symbol.upper(), date)
                download_file(checksum_path, checksum_file_name, date_range, folder)


def download_monthly_trades(trading_type, symbols, num_symbols, years, months,
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

    _logger.info("Starting monthly trades — %d symbols", num_symbols)

    for idx, symbol in enumerate(symbols, 1):
        _logger.info("[%d/%d] Monthly trades: %s", idx, num_symbols, symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _download_symbol_monthly,
                trading_type, symbol, years, months,
                start_date, end_date, date_range, folder, checksum
            )
            try:
                future.result(timeout=symbol_timeout)
            except concurrent.futures.TimeoutError:
                _logger.warning("TIMEOUT     | %s exceeded %ds — skipping to next symbol", symbol, symbol_timeout)
            except Exception as e:
                _logger.error("ERROR       | %s — %s", symbol, e)


def download_daily_trades(trading_type, symbols, num_symbols, dates,
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

    _logger.info("Starting daily trades — %d symbols", num_symbols)

    for idx, symbol in enumerate(symbols, 1):
        _logger.info("[%d/%d] Daily trades: %s", idx, num_symbols, symbol)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _download_symbol_daily,
                trading_type, symbol, dates,
                start_date, end_date, date_range, folder, checksum
            )
            try:
                future.result(timeout=symbol_timeout)
            except concurrent.futures.TimeoutError:
                _logger.warning("TIMEOUT     | %s exceeded %ds — skipping to next symbol", symbol, symbol_timeout)
            except Exception as e:
                _logger.error("ERROR       | %s — %s", symbol, e)


if __name__ == "__main__":
    parser = get_parser('trades')
    args = parser.parse_args(sys.argv[1:])

    setup_logging(log_path=args.log_file)

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
            download_monthly_trades(
                args.type, symbols, num_symbols, args.years, args.months,
                args.startDate, args.endDate, args.folder, args.checksum,
                args.symbol_timeout
            )

    if args.skip_daily == 0:
        download_daily_trades(
            args.type, symbols, num_symbols, dates,
            args.startDate, args.endDate, args.folder, args.checksum,
            args.symbol_timeout
        )
