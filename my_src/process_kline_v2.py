import os
import sys

from psycopg2 import sql

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..'))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from my_src.bulk_loader import (
    DB_NAME, parse_date_range_str, discover_monthly_work,
    prepare_bulk_load, finalize_bulk_load, run_worker_pool,
)
from my_src.sql_connection import create_connection

TMP_RAW_TABLE = 'tmp_kline_raw'


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

    # compress_after arg is an independent interval in microseconds
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
    """COPY one CSV file into the raw temp table, transform and upsert into the hypertable."""
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
    month_work, _, total_files = discover_monthly_work(
        tokens_dir, date_range, start_date, end_date, freq=freq
    )

    print(f"Discovered {total_files} files to process.")
    if not month_work:
        print("No work found for the given date range and folder path.")
        return

    skip_checksum = os.environ.get('KLINE_SKIP_CHECKSUM', '0').lower() in ('1', 'true', 'yes')
    if skip_checksum:
        print('KLINE_SKIP_CHECKSUM=1 -> skipping checksum verification.')

    table_name = f"spot_klines_{freq}"
    index_name = f"{table_name}_symbol_open_time_idx"

    conn = create_connection(DB_NAME)
    try:
        ensure_tables(conn, table_name)
    except Exception as e:
        print(f"Error ensuring tables: {e}", file=sys.stderr)
        conn.close()
        return
    conn.close()

    print("Preparing table for bulk load (UNLOGGED, index dropped)...")
    prepare_bulk_load(table_name, index_name)

    num_workers_env = os.environ.get('KLINE_WORKERS')
    num_workers = int(num_workers_env) if num_workers_env else min(12, max(1, os.cpu_count() or 1))

    errors = run_worker_pool(
        month_work=month_work,
        total_files=total_files,
        db_name=DB_NAME,
        skip_checksum=skip_checksum,
        num_workers=num_workers,
        setup_session_fn=prepare_ingest_session,
        process_file_fn=lambda conn, stream, token: copy_csv_and_flush(stream, token, conn, table_name),
    )

    if errors:
        print(f"\nCompleted with {len(errors)} errors. See logs above.", file=sys.stderr)

    print("\nFinalising bulk load...")
    finalize_bulk_load(table_name, index_name, 'symbol, open_time')

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
