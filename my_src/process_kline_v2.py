import os
from tqdm import tqdm
import sys
from datetime import datetime, timezone, date, timedelta

sys.path.append('../')

from my_src.file_utils import parse_file, verify_checksum, unzip_file
from my_src.sql_connection import create_connection


DB_NAME = 'binance_marketdata'


def days_range(start_date: date, end_date: date):
    """Yield list of date objects from start_date to end_date inclusive."""
    days = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur = cur + timedelta(days=1)
    return days


def ensure_table_exists_and_base_partition(conn, table_name: str):
    cur = conn.cursor()
    # Create database if not exists
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    cur.execute(f"USE `{DB_NAME}`;")
    # Ensure the session operates in UTC so FROM_UNIXTIME(...) yields UTC datetime
    try:
        cur.execute("SET time_zone = '+00:00';")
    except Exception:
        # If the server doesn't allow setting time_zone, proceed but note behavior may vary
        pass

    # Create the single unified table with a generated year_month column and a MAXVALUE partition placeholder.
    # open_time is stored in microseconds (16-digit epoch). open_date is derived from open_time/1e6 as DATE.
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      `symbol` VARCHAR(64) NOT NULL,
      `open_time` BIGINT UNSIGNED NOT NULL,
      `open` DOUBLE NOT NULL,
      `high` DOUBLE NOT NULL,
      `low` DOUBLE NOT NULL,
      `close` DOUBLE NOT NULL,
      `volume` DOUBLE NOT NULL,
      `close_time` BIGINT UNSIGNED NOT NULL,
      `quote_volume` DOUBLE NOT NULL,
      `count` INT NOT NULL,
      `taker_buy_base_volume` DOUBLE NOT NULL,
      `taker_buy_quote_volume` DOUBLE NOT NULL,
      `open_date` DATE AS (DATE(FROM_UNIXTIME(open_time/1000000))) STORED,
      -- Include partitioning column `open_date` in the PRIMARY KEY to satisfy MySQL partitioning rules
      PRIMARY KEY (`symbol`, `open_date`, `open_time`),
      KEY `idx_open_time` (`open_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    PARTITION BY RANGE COLUMNS(open_date) (
      PARTITION pmax VALUES LESS THAN (MAXVALUE)
    );
    """)
    conn.commit()
    cur.close()


def get_existing_partitions(conn, table_name: str):
    cur = conn.cursor()
    cur.execute(f"SELECT PARTITION_NAME, PARTITION_DESCRIPTION FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND PARTITION_NAME IS NOT NULL;", (DB_NAME, table_name))
    rows = cur.fetchall()
    cur.close()
    # build set of less-than values where applicable
    existing = set()
    for pname, pdesc in rows:
        # pdesc may be a string or numeric representation, or None for MAXVALUE
        if pdesc is None:
            existing.add('MAXVALUE')
            continue
        s = str(pdesc).strip()
        if s.upper() == 'MAXVALUE':
            existing.add('MAXVALUE')
            continue
        s = s.strip("'\"")
        # try to parse into a date and normalize to ISO 'YYYY-MM-DD'
        parsed = None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                parsed = datetime.strptime(s, fmt).date()
                break
            except Exception:
                continue
        if parsed:
            existing.add(parsed.isoformat())
        else:
            # fallback to raw string
            existing.add(s)
    return existing


def get_latest_partition_date(conn, table_name: str):
    """Return the maximum partition DESCRIPTION as a date object, or None if none (excluding MAXVALUE)."""
    cur = conn.cursor()
    cur.execute("SELECT PARTITION_DESCRIPTION FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND PARTITION_DESCRIPTION IS NOT NULL;", (DB_NAME, table_name))
    rows = cur.fetchall()
    cur.close()
    max_date = None
    for (pdesc,) in rows:
        if pdesc is None:
            continue
        s = str(pdesc).strip()
        if s.upper() == 'MAXVALUE':
            continue
        s = s.strip("'\"")
        parsed = None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                parsed = datetime.strptime(s, fmt).date()
                break
            except Exception:
                continue
        if parsed:
            if (max_date is None) or (parsed > max_date):
                max_date = parsed
    return max_date


def add_day_partitions(conn, table_name: str, days):
    """Ensure that each date in days (date objects) has a partition. Adds partitions by reorganizing the pmax partition.

    For each day D we create a partition with VALUES LESS THAN ('D_plus_1') where D_plus_1 is next day's date.
    """
    if not days:
        return
    # normalize and sort unique
    days = sorted({d for d in days})
    cur = conn.cursor()

    existing = get_existing_partitions(conn, table_name)
    latest = get_latest_partition_date(conn, table_name)
    # only add days strictly after the latest existing partition's less-than value
    if latest is not None:
        eligible_days = [d for d in days if d > latest]
    else:
        eligible_days = days

    # filter out any days already present
    partitions_to_add = [d for d in eligible_days if d.isoformat() not in existing]
    if not partitions_to_add:
        cur.close()
        return

    # Build a single REORGANIZE statement that adds all new partitions in ascending order
    # Ensure the first partition we add is strictly after existing max partition
    existing_max = get_latest_partition_date(conn, table_name)
    # drop days that are <= existing_max (safety)
    if existing_max is not None:
        while partitions_to_add and partitions_to_add[0] <= existing_max:
            partitions_to_add.pop(0)
    if not partitions_to_add:
        cur.close()
        return

    parts_sql = []
    for d in partitions_to_add:
        next_day = d + timedelta(days=1)
        less_than_str = next_day.isoformat()
        part_name = f"p{d.strftime('%Y%m%d')}"
        parts_sql.append(f"PARTITION `{part_name}` VALUES LESS THAN ('{less_than_str}')")

    # Append the pmax partition at the end
    parts_sql.append("PARTITION pmax VALUES LESS THAN (MAXVALUE)")
    alter_sql = f"ALTER TABLE `{table_name}` REORGANIZE PARTITION pmax INTO ({', '.join(parts_sql)});"
    cur.execute(f"USE `{DB_NAME}`;")
    cur.execute(alter_sql)
    conn.commit()
    # update existing set
    for d in partitions_to_add:
        existing.add(d.isoformat())
    cur.close()


def process_file(file_path, pair_name, sql_connection, table_name: str):
    klines = parse_file(file_path)
    if not klines:
        return

    # normalize and compute month coverage
    rows = []
    days = []
    for kline in klines:
        # Ensure epoch values are ints (microseconds). parse_file/process_time in file_utils may return floats.
        open_time = int(kline['open_time'])
        close_time = int(kline['close_time'])
        rows.append((
            pair_name,
            open_time,
            float(kline['open']),
            float(kline['high']),
            float(kline['low']),
            float(kline['close']),
            float(kline['volume']),
            close_time,
            float(kline['quote_volume']),
            int(kline['count']),
            float(kline['taker_buy_base_volume']),
            float(kline['taker_buy_quote_volume'])
        ))
        # compute the UTC date (day) for partitioning
        dt = datetime.fromtimestamp(open_time / 1_000_000, timezone.utc)
        days.append(dt.date())

    # Ensure table exists and base partition
    ensure_table_exists_and_base_partition(sql_connection, table_name)

    # Add needed partitions for months in this file
    min_day = min(days)
    max_day = max(days)
    needed = days_range(min_day, max_day)
    add_day_partitions(sql_connection, table_name, needed)

    # Bulk insert
    cur = sql_connection.cursor()
    insert_query = f"""
        INSERT IGNORE INTO `{table_name}` (
          symbol, open_time, open, high, low, close, volume, close_time, quote_volume, `count`, taker_buy_base_volume, taker_buy_quote_volume
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, rows)
    sql_connection.commit()
    cur.close()


def process_monthly(date_range: str, folder_path: str, freq: str='1m', market_type: str='spot', data_type: str='klines'):
    assert market_type in ['spot']
    assert data_type in ['klines']

    tokens_dir = os.path.join(folder_path, "data", market_type, "monthly", data_type)
    list_tokens = sorted(os.listdir(tokens_dir))

    sql_connection = create_connection(DB_NAME)
    # derive table name from freq (e.g., '1m' -> 'klines_1m')
    table_name = f"klines_{freq}"

    for token in tqdm(list_tokens, desc="Processing tokens"):
        zip_folder = os.path.join(tokens_dir, token, freq, date_range)
        all_files = os.listdir(zip_folder)
        all_file_names = set([f.split(".")[0] for f in all_files])
        for file_name in all_file_names:
            zip_file_path = os.path.join(zip_folder, f"{file_name}.zip")
            checksum_file_path = os.path.join(zip_folder, f"{file_name}.zip.CHECKSUM")
            # Step 1: Verify checksum (raises on mismatch)
            verify_checksum(zip_file_path, checksum_file_path)
            # Step 2: Unzip file
            csv_file_path = unzip_file(zip_file_path, zip_folder)
            try:
                # Step 3: Process CSV file into MySQL
                process_file(csv_file_path, token, sql_connection, table_name)
            finally:
                # Step 4: Remove CSV file after processing (keep zip and checksum intact)
                if os.path.exists(csv_file_path):
                    os.remove(csv_file_path)

    print('Finished, closing sql connection')
    sql_connection.close()


if __name__ == '__main__':
    # date_range = '2020-01-01_2025-09-30'
    # folder_path = "/home/m/data4/Downloads/binance_spot/1min_klines_test"
    # freq = '1m'
    # market_tyoe = 'spot'
    # data_type = 'klines'
    date_range, folder_path, freq, market_tyoe, data_type = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    process_monthly(date_range, folder_path, freq, market_tyoe, data_type)
