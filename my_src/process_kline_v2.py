import os
import sys
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime, date
from queue import Queue
from threading import Thread, Lock

from psycopg2 import sql
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..'))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from my_src.file_utils import verify_checksum, open_csv_from_zip
from my_src.sql_connection import create_connection


DB_NAME = 'binance_marketdata'
TMP_RAW_TABLE = 'tmp_kline_raw'


def _read_int_env(var_name: str, default: int) -> int:
    try:
        return int(os.environ.get(var_name, default))
    except (TypeError, ValueError):
        return default


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


def discover_monthly_work(
    tokens_dir: str, freq: str, date_range: str, start_month: date, end_month: date
) -> tuple[dict[date, list[FileTask]], dict[str, int], int]:
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


def ensure_tables(conn, table_name: str, chunk_days: int = 30, compress_after_days: int = 90):
    """
    Create the kline hypertable if it doesn't exist.

    chunk_days=30: 30-day chunks give ~43k rows/symbol/chunk for 1m data,
    which is in TimescaleDB's recommended range and reduces chunk count for
    backtesting range queries that span months.

    compress_after_days=90: compress chunks older than 90 days automatically.
    """
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table_name} (
              open_time         BIGINT NOT NULL,
              symbol            TEXT   NOT NULL,
              open              DOUBLE PRECISION NOT NULL,
              high              DOUBLE PRECISION NOT NULL,
              low               DOUBLE PRECISION NOT NULL,
              close             DOUBLE PRECISION NOT NULL,
              volume            DOUBLE PRECISION NOT NULL,
              close_time        BIGINT NOT NULL,
              quote_volume      DOUBLE PRECISION NOT NULL,
              count             BIGINT NOT NULL,
              taker_buy_base_volume  DOUBLE PRECISION NOT NULL,
              taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
              PRIMARY KEY (open_time, symbol)
            );
            """
        ).format(table_name=sql.Identifier(table_name))
    )

    # open_time is stored in microseconds; chunk interval in same unit
    chunk_interval_us = chunk_days * 24 * 60 * 60 * 1_000_000
    cur.execute(
        """
        SELECT create_hypertable(%s::regclass, 'open_time',
            chunk_time_interval => %s, if_not_exists => TRUE);
        """,
        (table_name, chunk_interval_us),
    )

    cur.execute(
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx} ON {table_name} (symbol, open_time);"
        ).format(
            idx=sql.Identifier(f"{table_name}_symbol_open_time_idx"),
            table_name=sql.Identifier(table_name),
        )
    )

    # compress_orderby ASC matches how range scans decompress data (oldest first)
    cur.execute(
        sql.SQL(
            "ALTER TABLE {table_name} SET ("
            "  timescaledb.compress = true,"
            "  timescaledb.compress_segmentby = 'symbol',"
            "  timescaledb.compress_orderby = 'open_time ASC'"
            ");"
        ).format(table_name=sql.Identifier(table_name))
    )

    # Fixed: compress_after arg is an independent interval in microseconds,
    # not a multiple of chunk_interval_us (that was a bug in the previous version).
    compress_after_us = compress_after_days * 24 * 60 * 60 * 1_000_000
    cur.execute(
        """
        SELECT job_id FROM timescaledb_information.jobs
        WHERE hypertable_name = %s AND application_name = 'Compression Policy';
        """,
        (table_name,),
    )
    if not cur.fetchone():
        cur.execute(
            "SELECT add_compression_policy(%s, %s, if_not_exists => TRUE);",
            (table_name, compress_after_us),
        )

    # Disable autovacuum during bulk load — re-enabled in finalize_bulk_load
    cur.execute(
        sql.SQL(
            "ALTER TABLE {table_name} SET ("
            "  autovacuum_enabled = false,"
            "  toast.autovacuum_enabled = false"
            ");"
        ).format(table_name=sql.Identifier(table_name))
    )
    conn.commit()
    cur.close()


def prepare_bulk_load(table_name: str):
    """
    Optimise the table for maximum INSERT throughput before loading.

    - UNLOGGED: skips WAL entirely during load (biggest single speedup, 3-5x)
    - DROP secondary index: rebuilding after load is far faster than maintaining
      the index per-row during millions of inserts
    """
    conn = create_connection(DB_NAME)
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {t} SET UNLOGGED;").format(t=sql.Identifier(table_name))
            )
            cur.execute(
                sql.SQL("DROP INDEX IF EXISTS {idx};").format(
                    idx=sql.Identifier(f"{table_name}_symbol_open_time_idx")
                )
            )
        conn.commit()
    finally:
        conn.close()


def finalize_bulk_load(table_name: str):
    """
    Restore durability, rebuild index, compress all chunks, and analyse.

    Call this once after all worker threads have completed.
    """
    conn = create_connection(DB_NAME)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Restore WAL so the table is crash-safe again
            print(f"  SET LOGGED on {table_name}...")
            cur.execute(
                sql.SQL("ALTER TABLE {t} SET LOGGED;").format(t=sql.Identifier(table_name))
            )

        with conn.cursor() as cur:
            # Rebuild secondary index as one bulk sort — much faster than row-by-row maintenance
            print(f"  Rebuilding index on {table_name}...")
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {idx} ON {t} (symbol, open_time);"
                ).format(
                    idx=sql.Identifier(f"{table_name}_symbol_open_time_idx"),
                    t=sql.Identifier(table_name),
                )
            )

        with conn.cursor() as cur:
            # Compress all chunks immediately rather than waiting for the background policy
            print(f"  Compressing all chunks of {table_name}...")
            cur.execute(
                "SELECT compress_chunk(c, if_not_compressed => TRUE) "
                "FROM show_chunks(%s) AS c;",
                (table_name,),
            )

        with conn.cursor() as cur:
            # Re-enable autovacuum and run an initial ANALYZE so the planner has stats
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {t} SET ("
                    "  autovacuum_enabled = true,"
                    "  toast.autovacuum_enabled = true"
                    ");"
                ).format(t=sql.Identifier(table_name))
            )

        with conn.cursor() as cur:
            print(f"  VACUUM ANALYZE {table_name}...")
            cur.execute(
                sql.SQL("VACUUM ANALYZE {t};").format(t=sql.Identifier(table_name))
            )
    finally:
        conn.close()


def prepare_ingest_session(conn):
    """Set session parameters and create the per-connection raw staging temp table."""
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit TO OFF;")
        cur.execute("SET temp_buffers = '64MB';")
        cur.execute("SET work_mem = '256MB';")
        cur.execute("SET maintenance_work_mem = '1GB';")
        cur.execute(
            sql.SQL(
                """
                CREATE TEMP TABLE IF NOT EXISTS {raw_table} (
                  open_time_text         TEXT,
                  open                   DOUBLE PRECISION,
                  high                   DOUBLE PRECISION,
                  low                    DOUBLE PRECISION,
                  close                  DOUBLE PRECISION,
                  volume                 DOUBLE PRECISION,
                  close_time_text        TEXT,
                  quote_volume           DOUBLE PRECISION,
                  count                  BIGINT,
                  taker_buy_base_volume  DOUBLE PRECISION,
                  taker_buy_quote_volume DOUBLE PRECISION,
                  ignore                 TEXT
                ) ON COMMIT DELETE ROWS;
                """
            ).format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
        cur.execute(
            sql.SQL("TRUNCATE {raw_table};").format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
    conn.commit()


def copy_csv_and_flush(csv_stream, token: str, conn, table_name: str):
    """
    COPY one CSV file into the raw temp table, transform and insert directly
    into the target hypertable, then clear the temp table.

    Eliminates the intermediate staging table and its per-file COUNT(*) check.
    """
    with conn.cursor() as cur:
        cur.copy_expert(
            sql.SQL("COPY {raw_table} FROM STDIN WITH (FORMAT CSV)").format(
                raw_table=sql.Identifier(TMP_RAW_TABLE)
            ),
            csv_stream,
        )
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {table_name}
                  (open_time, symbol, open, high, low, close, volume,
                   close_time, quote_volume, count,
                   taker_buy_base_volume, taker_buy_quote_volume)
                SELECT
                  CASE
                    WHEN length(open_time_text) = 13 THEN open_time_text::BIGINT * 1000
                    WHEN length(open_time_text) = 16 THEN open_time_text::BIGINT
                    ELSE NULL
                  END,
                  %s,
                  open, high, low, close, volume,
                  CASE
                    WHEN length(close_time_text) = 13 THEN close_time_text::BIGINT * 1000
                    WHEN length(close_time_text) = 16 THEN close_time_text::BIGINT
                    ELSE NULL
                  END,
                  quote_volume, count,
                  taker_buy_base_volume, taker_buy_quote_volume
                FROM {raw_table}
                WHERE open_time_text IS NOT NULL
                  AND close_time_text IS NOT NULL
                ON CONFLICT (open_time, symbol) DO NOTHING;
                """
            ).format(
                table_name=sql.Identifier(table_name),
                raw_table=sql.Identifier(TMP_RAW_TABLE),
            ),
            (token,),
        )
        cur.execute(
            sql.SQL("TRUNCATE {raw_table};").format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
    conn.commit()


def process_monthly(
    date_range: str,
    folder_path: str,
    freq: str = '1m',
    market_type: str = 'spot',
    data_type: str = 'klines',
):
    start_date, end_date = parse_date_range_str(date_range)

    tokens_dir = os.path.join(folder_path, 'data', market_type, 'monthly', data_type)
    month_work, token_file_counts, total_files = discover_monthly_work(
        tokens_dir, freq, date_range, start_date, end_date
    )

    print(f"Discovered {total_files} files to process.")
    if not month_work:
        print("No work found for the given date range and folder path.")
        return

    skip_checksum = os.environ.get('KLINE_SKIP_CHECKSUM', '0').lower() in ('1', 'true', 'yes')
    if skip_checksum:
        print('KLINE_SKIP_CHECKSUM=1 -> skipping checksum verification.')

    table_name = f"klines_{freq}"

    # -- Table setup --------------------------------------------------------
    conn = create_connection(DB_NAME)
    try:
        ensure_tables(conn, table_name)
    except Exception as e:
        print(f"Error ensuring tables: {e}", file=sys.stderr)
        conn.close()
        return
    conn.close()

    # -- Bulk-load optimisation: UNLOGGED + drop secondary index ------------
    print("Preparing table for bulk load (UNLOGGED, index dropped)...")
    prepare_bulk_load(table_name)

    # -- Worker pool --------------------------------------------------------
    total_workers = os.environ.get('KLINE_WORKERS')
    if total_workers is None:
        cpu_guess = os.cpu_count() or 1
        total_workers = min(12, max(1, cpu_guess))
    else:
        total_workers = max(1, int(total_workers))

    task_queue: Queue[tuple[str, FileTask] | None] = Queue()
    progress_lock = Lock()
    error_lock = Lock()
    worker_stats: Counter[int] = Counter()
    worker_last_active: dict[int, float] = {}
    errors: list[str] = []

    for month, tasks in sorted(month_work.items()):
        month_str = month.strftime('%Y-%m')
        for task in tasks:
            task_queue.put((month_str, task))

    def worker_main(worker_id: int, pbar):
        conn = create_connection(DB_NAME)
        try:
            prepare_ingest_session(conn)
            while True:
                item = task_queue.get()
                if item is None:
                    task_queue.task_done()
                    break
                month_str, task = item
                try:
                    if not skip_checksum and os.path.exists(task.checksum_path):
                        verify_checksum(task.zip_path, task.checksum_path)

                    with open_csv_from_zip(task.zip_path) as csv_stream:
                        copy_csv_and_flush(csv_stream, task.token, conn, table_name)

                except Exception as exc:
                    msg = f"Worker {worker_id} failed for {task.token} ({month_str}): {exc}"
                    print(msg, file=sys.stderr)
                    with error_lock:
                        errors.append(msg)
                finally:
                    with progress_lock:
                        worker_stats[worker_id] += 1
                        worker_last_active[worker_id] = time.monotonic()
                        queue_depth = task_queue.qsize()
                        busiest = max(worker_last_active, key=worker_last_active.get)
                        pbar.set_postfix({
                            "last": worker_id,
                            "busy": busiest,
                            "queue": queue_depth,
                        })
                        pbar.update(1)
                    task_queue.task_done()
        finally:
            conn.close()

    with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
        workers: list[Thread] = []
        for idx in range(total_workers):
            thread = Thread(target=worker_main, args=(idx + 1, pbar), daemon=True)
            thread.start()
            workers.append(thread)

        for _ in workers:
            task_queue.put(None)

        task_queue.join()
        for thread in workers:
            thread.join()

    if errors:
        print(f"\nCompleted with {len(errors)} errors. See logs above.", file=sys.stderr)

    # -- Post-load: restore durability, rebuild index, compress, analyse ---
    print("\nFinalising bulk load...")
    finalize_bulk_load(table_name)

    try:
        with open(os.path.join(folder_path, 'success_marker.txt'), 'a'):
            pass
    except Exception as e:
        print(f"Warning: could not write success marker: {e}", file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        prog = os.path.basename(sys.argv[0])
        print(
            f"Usage: {prog} <DATE_RANGE> <FOLDER_PATH> <FREQ> <MARKET_TYPE> <DATA_TYPE>\n"
            "Example: process_kline_v2.py 2020-01-01_2020-02-01 /data/binance 1m spot klines",
            file=sys.stderr,
        )
        sys.exit(1)

    process_monthly(
        date_range=sys.argv[1],
        folder_path=sys.argv[2],
        freq=sys.argv[3],
        market_type=sys.argv[4],
        data_type=sys.argv[5],
    )
