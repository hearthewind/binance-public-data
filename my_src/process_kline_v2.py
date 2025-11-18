import os
from tqdm import tqdm
import sys
from datetime import datetime, timezone, date
import threading
import queue
from collections import defaultdict

sys.path.append('../')

from my_src.file_utils import verify_checksum, unzip_file, process_time
from my_src.sql_connection import create_connection


DB_NAME = 'binance_marketdata'
STAGING_SUFFIX = '_staging'


def first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def months_range(start_date: date, end_date: date):
    start_m = first_of_month(start_date)
    end_m = first_of_month(end_date)
    cur = start_m
    while cur <= end_m:
        yield cur
        cur = next_month(cur)


def parse_base_month(base: str) -> date | None:
    """Extract YYYY-MM info embedded in Binance filenames such as SYMBOL-1m-2020-01."""
    parts = base.split('-')
    if len(parts) < 4:
        return None
    year_part = parts[-2]
    month_part = parts[-1]
    if len(year_part) != 4 or len(month_part) not in (1, 2):
        return None
    try:
        year = int(year_part)
        month = int(month_part)
        return date(year, month, 1)
    except ValueError:
        return None


def date_to_epoch_us(d: date) -> int:
    """Start-of-day UTC epoch in microseconds for the given date."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1_000_000)


def parse_date_range_str(s: str) -> tuple[date, date]:
    """Parse 'YYYY-MM-DD_YYYY-MM-DD' into (start_date, end_date)."""
    s_date, e_date = s.split('_')
    sd = datetime.strptime(s_date, '%Y-%m-%d').date()
    ed = datetime.strptime(e_date, '%Y-%m-%d').date()
    if ed < sd:
        raise ValueError('end_date earlier than start_date')
    return sd, ed


def staging_table_name(table_name: str) -> str:
    return f"{table_name}{STAGING_SUFFIX}"


def ensure_table_created_with_partitions(conn, table_name: str, start_day: date, end_day: date) -> str:
    """Create schema artifacts and ensure the table has monthly partitions.

    Every token shares one table; we keep the VARCHAR symbol next to the timestamps so downstream
    queries stay simple. Monthly range partitions keep the partition count manageable while still
    allowing efficient open_time pruning. Data is first bulk-loaded into a staging table and then
    swapped into the target partition to avoid quadratic insert cost as the table grows."""

    def ensure_database(cur):
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cur.execute(f"USE `{DB_NAME}`;")

    columns_sql = """
              `open_time` BIGINT UNSIGNED NOT NULL,
              `symbol` VARCHAR(64) NOT NULL,
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
              PRIMARY KEY (`open_time`, `symbol`),
              KEY `idx_symbol_time` (`symbol`, `open_time`)
    """

    def ensure_klines_table(cur):
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
            {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            PARTITION BY RANGE (`open_time`)
            (
              PARTITION `p00000000` VALUES LESS THAN (0),
              PARTITION `pmax` VALUES LESS THAN (MAXVALUE)
            );
            """
        )

    def ensure_staging_table(cur, staging_name: str):
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{staging_name}` (
            {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )

    cur = conn.cursor()
    ensure_database(cur)
    ensure_klines_table(cur)
    staging = staging_table_name(table_name)
    ensure_staging_table(cur, staging)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = 'symbol';
        """,
        (DB_NAME, table_name)
    )
    if cur.fetchone()[0] == 0:
        raise RuntimeError(
            f"Existing table `{DB_NAME}`.`{table_name}` needs a VARCHAR `symbol` column. "
            "Drop/rename it or add the column before running the loader."
        )
    conn.commit()
    cur.close()

    ensure_monthly_partitions(conn, table_name, start_day, end_day)
    return staging


def ensure_monthly_partitions(conn, table_name: str, start_day: date, end_day: date, batch_size: int = 48):
    """Ensure [start_day, end_day] months each have a dedicated partition."""
    if end_day < start_day:
        return

    needed = []
    for month_start in months_range(start_day, end_day):
        pname = f"p{month_start.strftime('%Y%m')}"
        upper = date_to_epoch_us(next_month(month_start))
        needed.append((pname, upper))

    cur = conn.cursor()
    cur.execute(
        """
        SELECT PARTITION_NAME
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND PARTITION_NAME IS NOT NULL;
        """,
        (DB_NAME, table_name)
    )
    existing = {row[0] for row in cur.fetchall() if row[0]}
    cur.close()

    missing = [(name, upper) for name, upper in needed if name not in existing]
    if not missing:
        return

    start_idx = 0
    while start_idx < len(missing):
        chunk = missing[start_idx:start_idx + batch_size]
        part_sql = ",\n          ".join(
            f"PARTITION `{name}` VALUES LESS THAN ({upper})" for name, upper in chunk
        )
        alter_sql = f"""
            ALTER TABLE `{table_name}`
            REORGANIZE PARTITION `pmax` INTO (
              {part_sql},
              PARTITION `pmax` VALUES LESS THAN (MAXVALUE)
            );
        """
        cur = conn.cursor()
        cur.execute(alter_sql)
        conn.commit()
        cur.close()
        start_idx += batch_size


def truncate_table(conn, table_name: str):
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE `{table_name}`;")
    conn.commit()
    cur.close()


def exchange_partition_into_main(conn, table_name: str, partition_name: str, staging_table: str):
    cur = conn.cursor()
    cur.execute(
        f"ALTER TABLE `{table_name}` EXCHANGE PARTITION `{partition_name}` "
        f"WITH TABLE `{staging_table}` WITHOUT VALIDATION;"
    )
    conn.commit()
    cur.close()


def check_local_infile_enabled(conn):
    cur = conn.cursor()
    cur.execute("SHOW VARIABLES LIKE 'local_infile';")
    row = cur.fetchone()
    cur.close()
    if not row:
        return False
    name, val = row
    return str(val).lower() in ('on', '1', 'true', 'yes')


def tune_session_for_bulk_load(cur):
    """Apply session-level settings that significantly speed up bulk loads.
    Requires non-replicated environment. Adjust if using replication.
    """
    stmts = [
        "SET time_zone = '+00:00'",
        "SET SESSION unique_checks = 0",
        "SET SESSION foreign_key_checks = 0",
        "SET SESSION sql_log_bin = 0",
        "SET SESSION innodb_flush_log_at_trx_commit = 2",
        "SET SESSION sync_binlog = 0",
    ]
    for s in stmts:
        try:
            cur.execute(s)
        except Exception:
            # Ignore if not permitted
            pass


def load_csv_into_mysql(csv_file_path: str, token: str, conn, table_name: str) -> int:
    """Fast bulk load a Binance kline CSV into MySQL using LOAD DATA LOCAL INFILE."""

    def esc(s: str) -> str:
        return s.replace('\\', r'\\').replace("'", r"\'")

    file_lit = esc(csv_file_path)
    token_lit = esc(token)
    load_sql = f"""
        LOAD DATA LOCAL INFILE '{file_lit}' IGNORE INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        (@ot, @o, @h, @l, @c, @v, @ct, @qv, @cnt, @tbv, @tbqv, @ign)
        SET
          `symbol` = '{token_lit}',
          `open_time` = CASE
              WHEN CHAR_LENGTH(@ot) = 13 THEN CAST(@ot AS UNSIGNED) * 1000
              WHEN CHAR_LENGTH(@ot) = 16 THEN CAST(@ot AS UNSIGNED)
              ELSE NULL
          END,
          `open` = @o,
          `high` = @h,
          `low` = @l,
          `close` = @c,
          `volume` = @v,
          `close_time` = CASE
              WHEN CHAR_LENGTH(@ct) = 13 THEN CAST(@ct AS UNSIGNED) * 1000
              WHEN CHAR_LENGTH(@ct) = 16 THEN CAST(@ct AS UNSIGNED)
              ELSE NULL
          END,
          `quote_volume` = @qv,
          `count` = @cnt,
          `taker_buy_base_volume` = @tbv,
          `taker_buy_quote_volume` = @tbqv;
    """
    cur = conn.cursor()
    cur.execute(load_sql)
    affected = cur.rowcount if hasattr(cur, 'rowcount') else -1
    conn.commit()
    cur.close()
    return affected


def load_csv_via_insert(csv_file_path: str, token: str, conn, table_name: str, batch_size: int = 5000) -> int:
    """Fallback loader: stream the CSV in Python and issue batched INSERT IGNORE statements.
    Returns total rows attempted (best-effort; IGNORE may drop duplicates).
    """
    total = 0
    rows = []
    cur = conn.cursor()
    sql = f"""
        INSERT IGNORE INTO `{table_name}`
        (`open_time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `close_time`, `quote_volume`, `count`, `taker_buy_base_volume`, `taker_buy_quote_volume`)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with open(csv_file_path, 'r') as f:
        for line in f:
            parts = line.rstrip('\n').split(',')
            if len(parts) < 12:
                continue
            ot = process_time(parts[0])
            ct = process_time(parts[6])
            if ot is None or ct is None:
                continue
            try:
                o = float(parts[1]); h = float(parts[2]); l = float(parts[3]); c = float(parts[4])
                v = float(parts[5]); qv = float(parts[7]); cnt = int(parts[8])
                tbv = float(parts[9]); tbqv = float(parts[10])
            except Exception:
                continue
            rows.append((ot, token, o, h, l, c, v, ct, qv, cnt, tbv, tbqv))
            if len(rows) >= batch_size:
                cur.executemany(sql, rows)
                conn.commit()
                total += len(rows)
                rows.clear()
    if rows:
        cur.executemany(sql, rows)
        conn.commit()
        total += len(rows)
    cur.close()
    return total


def process_monthly(date_range: str, folder_path: str, freq: str='1m', market_type: str='spot', data_type: str='klines'):
    assert market_type in ['spot']
    assert data_type in ['klines']

    tokens_dir = os.path.join(folder_path, "data", market_type, "monthly", data_type)
    list_tokens = sorted(os.listdir(tokens_dir))

    # derive table name from freq (e.g., '1m' -> 'klines_1m')
    table_name = f"klines_{freq}"

    # Parse the overall date range once and precreate all needed partitions
    start_day, end_day = parse_date_range_str(date_range)
    month_work: dict[date, list[tuple[str, str, str, str]]] = defaultdict(list)
    token_file_counts: dict[str, int] = defaultdict(int)
    total_files = 0

    month_start_bound = first_of_month(start_day)
    month_end_bound = first_of_month(end_day)

    for token in list_tokens:
        zip_folder = os.path.join(tokens_dir, token, freq, date_range)
        if not os.path.isdir(zip_folder):
            continue
        all_files = os.listdir(zip_folder)
        base_names = sorted({f.split('.')[0] for f in all_files})
        for base in base_names:
            month_start = parse_base_month(base)
            if month_start is None:
                continue
            if month_start < month_start_bound or month_start > month_end_bound:
                continue
            zip_file_path = os.path.join(zip_folder, f"{base}.zip")
            checksum_file_path = os.path.join(zip_folder, f"{base}.zip.CHECKSUM")
            if not (os.path.exists(zip_file_path) and os.path.exists(checksum_file_path)):
                continue
            month_work[month_start].append((token, zip_file_path, checksum_file_path, zip_folder))
            token_file_counts[token] += 1
            total_files += 1

    if total_files == 0:
        print(f"No files found under {tokens_dir}/<TOKEN>/{freq}/{date_range}. Nothing to load.")
        return

    skip_checksum = os.environ.get('KLINE_SKIP_CHECKSUM', '0').lower() in ('1', 'true', 'yes')
    if skip_checksum:
        print('KLINE_SKIP_CHECKSUM=1 -> skipping checksum verification.')

    meta_conn = create_connection(DB_NAME)
    if meta_conn is None:
        raise RuntimeError('Failed to establish metadata connection to MySQL.')

    try:
        # Diagnose LOCAL INFILE capability early, allow override via env to force fallback path
        force_insert = os.environ.get('KLINE_LOAD_FORCE_INSERT', '0') in ('1', 'true', 'True')
        use_local = check_local_infile_enabled(meta_conn) and not force_insert
        if not use_local:
            msg = "Using fallback batched INSERT loader (local_infile is OFF or forced). This is slower than LOAD DATA."
            print(msg)
        else:
            print("Using LOAD DATA LOCAL INFILE fast path.")

        staging_table = ensure_table_created_with_partitions(meta_conn, table_name, start_day, end_day)

        ordered_months = sorted(month_work.keys())
        print(
            f"Discovered {len(token_file_counts)} tokens, {total_files} files "
            f"across {len(ordered_months)} months to load into {DB_NAME}.{table_name}."
        )

        # Progress bars (thread-safe updates)
        file_pbar = tqdm(total=total_files, desc="Files", position=0, leave=True)
        token_pbar = tqdm(total=len(token_file_counts), desc="Tokens", position=1, leave=True)
        pbar_lock = threading.Lock()

        token_remaining = dict(token_file_counts)

        env_workers = os.environ.get('KLINE_LOAD_WORKERS')
        if env_workers:
            try:
                max_workers = max(1, int(env_workers))
            except Exception:
                max_workers = min(max(8, (os.cpu_count() or 4) // 2), 24)
        else:
            max_workers = min(24, max(8, (os.cpu_count() or 4)))

        task_queue: queue.Queue = queue.Queue(maxsize=max_workers * 4)
        stop_token = object()
        error_event = threading.Event()
        error_lock = threading.Lock()
        worker_errors: list[tuple[str, str, Exception]] = []

        def record_error(token: str, csv_file_path: str, exc: Exception):
            error_event.set()
            with error_lock:
                worker_errors.append((token, csv_file_path, exc))

        def worker_loop():
            conn = create_connection(DB_NAME)
            cur = conn.cursor()
            tune_session_for_bulk_load(cur)
            cur.close()
            try:
                while True:
                    item = task_queue.get()
                    if item is stop_token:
                        task_queue.task_done()
                        break
                    token, zip_file_path, checksum_file_path, zip_folder = item
                    try:
                        if not skip_checksum:
                            verify_checksum(zip_file_path, checksum_file_path)
                        csv_file_path = unzip_file(zip_file_path, zip_folder)
                        try:
                            target_table = staging_table
                            if use_local:
                                inserted = load_csv_into_mysql(csv_file_path, token, conn, target_table)
                            else:
                                inserted = load_csv_via_insert(csv_file_path, token, conn, target_table)
                            if inserted == 0:
                                try:
                                    wcur = conn.cursor()
                                    wcur.execute("SHOW WARNINGS LIMIT 5;")
                                    warns = wcur.fetchall()
                                    wcur.close()
                                    if warns:
                                        print(f"WARNINGS for {token} {os.path.basename(csv_file_path)}: {warns}")
                                except Exception:
                                    pass
                        finally:
                            if os.path.exists(csv_file_path):
                                try:
                                    os.remove(csv_file_path)
                                except Exception:
                                    pass
                        with pbar_lock:
                            file_pbar.update(1)
                            if token in token_remaining:
                                token_remaining[token] -= 1
                                if token_remaining[token] == 0:
                                    token_pbar.update(1)
                    except Exception as exc:
                        record_error(token, zip_file_path, exc)
                    finally:
                        task_queue.task_done()
            finally:
                conn.close()

        workers = []
        for _ in range(max_workers):
            thread = threading.Thread(target=worker_loop, name=f"loader-worker-{_}", daemon=True)
            thread.start()
            workers.append(thread)

        try:
            for month_start in ordered_months:
                tasks = month_work[month_start]
                if not tasks:
                    continue
                month_label = month_start.strftime('%Y-%m')
                partition_name = f"p{month_start.strftime('%Y%m')}"
                tqdm.write(f"Preparing staging table for {month_label} ...")
                truncate_table(meta_conn, staging_table)
                tqdm.write(f"Queueing {len(tasks)} files for {month_label} ...")
                for token, zip_file_path, checksum_file_path, zip_folder in tasks:
                    task_queue.put((token, zip_file_path, checksum_file_path, zip_folder))
                task_queue.join()
                if error_event.is_set():
                    break
                tqdm.write(f"Swapping staged data into partition {partition_name} ...")
                exchange_partition_into_main(meta_conn, table_name, partition_name, staging_table)
                truncate_table(meta_conn, staging_table)
        finally:
            for _ in workers:
                task_queue.put(stop_token)
            task_queue.join()
            for thread in workers:
                thread.join()

        file_pbar.close()
        token_pbar.close()

        if worker_errors:
            token, failing_file, exc = worker_errors[0]
            raise RuntimeError(f"Worker failed for {token} ({os.path.basename(failing_file)}): {exc}") from exc

        # Final verification count
        vcur = meta_conn.cursor()
        vcur.execute(f"SELECT COUNT(*) FROM `{table_name}`;")
        total_rows = vcur.fetchone()[0]
        vcur.close()
        print(f'Finished all loads. Table {DB_NAME}.{table_name} now has {total_rows} rows.')
    finally:
        try:
            meta_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    # date_range = '2020-01-01_2025-09-30'
    # folder_path = "/home/m/data4/Downloads/binance_spot/1min_klines_test"
    # freq = '1m'
    # market_tyoe = 'spot'
    # data_type = 'klines'
    date_range, folder_path, freq, market_tyoe, data_type = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    process_monthly(date_range, folder_path, freq, market_tyoe, data_type)
