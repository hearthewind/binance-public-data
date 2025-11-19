import os
import sys
import queue
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date

from psycopg2 import sql
from tqdm import tqdm

sys.path.append('../')

from my_src.file_utils import verify_checksum, unzip_file, process_time
from my_src.sql_connection import create_connection, ensure_database_ready


DB_NAME = 'binance_marketdata'


@dataclass(frozen=True)
class FileTask:
    token: str
    zip_path: str
    checksum_path: str
    extract_dir: str


def first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def months_range(start_date: date, end_date: date):
    cur = first_of_month(start_date)
    end = first_of_month(end_date)
    while cur <= end:
        yield cur
        cur = next_month(cur)


def parse_base_month(base: str) -> date | None:
    parts = base.split('-')
    if len(parts) < 4:
        return None
    try:
        year = int(parts[-2])
        month = int(parts[-1])
        return date(year, month, 1)
    except ValueError:
        return None


def parse_date_range_str(s: str) -> tuple[date, date]:
    start_str, end_str = s.split('_')
    start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
    if end_date < start_date:
        raise ValueError('end_date earlier than start_date')
    return start_date, end_date


def discover_monthly_work(tokens_dir: str, freq: str, date_range: str, start_month: date, end_month: date) -> tuple[dict[date, list[FileTask]], dict[str, int], int]:
    month_work: dict[date, list[FileTask]] = defaultdict(list)
    token_file_counts: dict[str, int] = defaultdict(int)
    total_files = 0

    tokens = sorted(os.listdir(tokens_dir))
    for token in tokens:
        zip_folder = os.path.join(tokens_dir, token, freq, date_range)
        if not os.path.isdir(zip_folder):
            continue
        base_names = sorted({f.split('.')[0] for f in os.listdir(zip_folder)})
        for base in base_names:
            month_marker = parse_base_month(base)
            if month_marker is None or month_marker < start_month or month_marker > end_month:
                continue
            zip_file_path = os.path.join(zip_folder, f"{base}.zip")
            checksum_file_path = os.path.join(zip_folder, f"{base}.zip.CHECKSUM")
            if not (os.path.exists(zip_file_path) and os.path.exists(checksum_file_path)):
                continue
            month_work[month_marker].append(FileTask(token, zip_file_path, checksum_file_path, zip_folder))
            token_file_counts[token] += 1
            total_files += 1

    return month_work, token_file_counts, total_files


def ensure_tables(conn, table_name: str, chunk_days: int = 7, compress_after_days: int = 30):
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table_name} (
              open_time BIGINT NOT NULL,
              symbol TEXT NOT NULL,
              open DOUBLE PRECISION NOT NULL,
              high DOUBLE PRECISION NOT NULL,
              low DOUBLE PRECISION NOT NULL,
              close DOUBLE PRECISION NOT NULL,
              volume DOUBLE PRECISION NOT NULL,
              close_time BIGINT NOT NULL,
              quote_volume DOUBLE PRECISION NOT NULL,
              count BIGINT NOT NULL,
              taker_buy_base_volume DOUBLE PRECISION NOT NULL,
              taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
              PRIMARY KEY (open_time, symbol)
            );
            """
        ).format(table_name=sql.Identifier(table_name))
    )

    chunk_interval_us = chunk_days * 24 * 60 * 60 * 1_000_000
    cur.execute(
        """
        SELECT create_hypertable(%s::regclass, 'open_time', chunk_time_interval => %s, if_not_exists => TRUE);
        """,
        (table_name, chunk_interval_us),
    )

    # Index optimized for typical symbol filtering
    cur.execute(
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx} ON {table_name} (symbol, open_time);"
        ).format(idx=sql.Identifier(f"{table_name}_symbol_open_time_idx"), table_name=sql.Identifier(table_name))
    )

    # Enable compression to keep old chunks small
    cur.execute(
        sql.SQL(
            "ALTER TABLE {table_name} SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'symbol', timescaledb.compress_orderby = 'open_time DESC');"
        ).format(table_name=sql.Identifier(table_name))
    )
    cur.execute(
        """
        SELECT job_id FROM timescaledb_information.jobs
        WHERE hypertable_name = %s AND application_name = 'Compression Policy';
        """,
        (table_name,),
    )
    if not cur.fetchone():
        cur.execute(
            """
            SELECT add_compression_policy(%s, %s, if_not_exists => TRUE);
            """,
            (table_name, compress_after_days * chunk_interval_us),
        )
    conn.commit()
    cur.close()


def prepare_ingest_session(conn):
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit TO OFF;")
        cur.execute("SET temp_buffers = '32MB';")
        cur.execute("SET work_mem = '64MB';")
        cur.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS tmp_kline_load (
              open_time_text TEXT,
              open DOUBLE PRECISION,
              high DOUBLE PRECISION,
              low DOUBLE PRECISION,
              close DOUBLE PRECISION,
              volume DOUBLE PRECISION,
              close_time_text TEXT,
              quote_volume DOUBLE PRECISION,
              count BIGINT,
              taker_buy_base_volume DOUBLE PRECISION,
              taker_buy_quote_volume DOUBLE PRECISION,
              ignore TEXT
            ) ON COMMIT DELETE ROWS;
            """
        )
    conn.commit()


def copy_csv_into_postgres(csv_file_path: str, token: str, conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE tmp_kline_load;")
        with open(csv_file_path, 'r') as f:
            cur.copy_expert("COPY tmp_kline_load FROM STDIN WITH (FORMAT CSV)", f)
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {table_name}
                (open_time, symbol, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_base_volume, taker_buy_quote_volume)
                SELECT
                  CASE
                    WHEN length(open_time_text) = 13 THEN open_time_text::BIGINT * 1000
                    WHEN length(open_time_text) = 16 THEN open_time_text::BIGINT
                    ELSE NULL
                  END AS open_time,
                  %s AS symbol,
                  open,
                  high,
                  low,
                  close,
                  volume,
                  CASE
                    WHEN length(close_time_text) = 13 THEN close_time_text::BIGINT * 1000
                    WHEN length(close_time_text) = 16 THEN close_time_text::BIGINT
                    ELSE NULL
                  END AS close_time,
                  quote_volume,
                  count,
                  taker_buy_base_volume,
                  taker_buy_quote_volume
                FROM tmp_kline_load
                WHERE open_time_text IS NOT NULL AND close_time_text IS NOT NULL
                ON CONFLICT (open_time, symbol) DO NOTHING;
                """
            ).format(table_name=sql.Identifier(table_name)),
            (token,),
        )
    conn.commit()


def process_monthly(date_range: str, folder_path: str, freq: str = '1m', market_type: str = 'spot', data_type: str = 'klines'):
    assert market_type in ['spot']
    assert data_type in ['klines']

    tokens_dir = os.path.join(folder_path, 'data', market_type, 'monthly', data_type)
    table_name = f"klines_{freq}"

    start_day, end_day = parse_date_range_str(date_range)
    month_start_bound = first_of_month(start_day)
    month_end_bound = first_of_month(end_day)

    month_work, token_file_counts, total_files = discover_monthly_work(
        tokens_dir, freq, date_range, month_start_bound, month_end_bound
    )

    if total_files == 0:
        print(f"No files found under {tokens_dir}/<TOKEN>/{freq}/{date_range}. Nothing to load.")
        return

    skip_checksum = os.environ.get('KLINE_SKIP_CHECKSUM', '0').lower() in ('1', 'true', 'yes')
    if skip_checksum:
        print('KLINE_SKIP_CHECKSUM=1 -> skipping checksum verification.')

    ensure_database_ready(DB_NAME)
    meta_conn = create_connection(DB_NAME)
    ensure_tables(meta_conn, table_name)
    meta_conn.close()

    ordered_months = sorted(month_work.keys())
    print(
        f"Discovered {len(token_file_counts)} tokens, {total_files} files "
        f"across {len(ordered_months)} months to load into {DB_NAME}.{table_name}."
    )

    file_pbar = tqdm(total=total_files, desc='Files', position=0, leave=True)
    token_pbar = tqdm(total=len(token_file_counts), desc='Tokens', position=1, leave=True)
    pbar_lock = threading.Lock()
    token_remaining = dict(token_file_counts)

    def determine_worker_count() -> int:
        env_workers = os.environ.get('KLINE_LOAD_WORKERS')
        if not env_workers:
            return min(24, max(8, (os.cpu_count() or 4)))
        try:
            return max(1, int(env_workers))
        except ValueError:
            return min(24, max(8, (os.cpu_count() or 4)))

    max_workers = determine_worker_count()

    task_queue: queue.Queue = queue.Queue(maxsize=max_workers * 4)
    stop_token = object()
    error_event = threading.Event()
    error_lock = threading.Lock()
    worker_errors: list[tuple[str, str, Exception]] = []

    def record_error(token: str, csv_path: str, exc: Exception):
        error_event.set()
        with error_lock:
            worker_errors.append((token, csv_path, exc))

    def worker_loop():
        conn = create_connection(DB_NAME)
        prepare_ingest_session(conn)
        try:
            while True:
                item = task_queue.get()
                if item is stop_token:
                    task_queue.task_done()
                    break
                task = item  # FileTask
                token = task.token
                try:
                    if not skip_checksum:
                        verify_checksum(task.zip_path, task.checksum_path)
                    csv_path = unzip_file(task.zip_path, task.extract_dir)
                    try:
                        copy_csv_into_postgres(csv_path, token, conn, table_name)
                    finally:
                        if os.path.exists(csv_path):
                            try:
                                os.remove(csv_path)
                            except OSError:
                                pass
                    with pbar_lock:
                        file_pbar.update(1)
                        if token in token_remaining:
                            token_remaining[token] -= 1
                            if token_remaining[token] == 0:
                                token_pbar.update(1)
                except Exception as exc:
                    record_error(token, task.zip_path, exc)
                finally:
                    task_queue.task_done()
        finally:
            conn.close()

    workers = []
    for i in range(max_workers):
        t = threading.Thread(target=worker_loop, name=f'loader-worker-{i}', daemon=True)
        t.start()
        workers.append(t)

    try:
        for month_start in ordered_months:
            tasks = month_work[month_start]
            if not tasks:
                continue
            tqdm.write(f"Queueing {len(tasks)} files for {month_start.strftime('%Y-%m')} ...")
            for task in tasks:
                task_queue.put(task)
            task_queue.join()
            if error_event.is_set():
                break
    finally:
        for _ in workers:
            task_queue.put(stop_token)
        task_queue.join()
        for t in workers:
            t.join()

    file_pbar.close()
    token_pbar.close()

    if worker_errors:
        token, failing_file, exc = worker_errors[0]
        raise RuntimeError(f"Worker failed for {token} ({os.path.basename(failing_file)}): {exc}") from exc

    verify_conn = create_connection(DB_NAME)
    vcur = verify_conn.cursor()
    vcur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table_name)))
    total_rows = vcur.fetchone()[0]
    vcur.close()
    verify_conn.close()
    print(f'Finished all loads. Table {DB_NAME}.{table_name} now has {total_rows} rows.')


if __name__ == '__main__':
    date_range, folder_path, freq, market_type, data_type = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    process_monthly(date_range, folder_path, freq, market_type, data_type)
