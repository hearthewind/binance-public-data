"""
Trade tick ingestion pipeline.

Reads monthly trade zip archives downloaded by the Binance public-data
downloader and bulk-loads them into a TimescaleDB hypertable.

Table naming convention: ``{market_type}_trades``
  e.g.  spot_trades,  futures_um_trades,  futures_cm_trades

Usage::

    python process_trades.py <DATE_RANGE> <FOLDER_PATH> <MARKET_TYPE> <DATA_TYPE>
    python process_trades.py 2020-01-01_2025-09-30 /data/binance/trade spot trades
    python process_trades.py 2020-01-01_2025-09-30 /data/binance/trade futures_um trades
"""
import os
import sys

from psycopg2 import sql

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..'))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from my_src.bulk_loader import (
    DB_NAME,
    parse_date_range_str,
    discover_monthly_work,
    prepare_bulk_load,
    finalize_bulk_load,
    run_worker_pool,
)
from my_src.sql_connection import create_connection


TMP_RAW_TABLE = 'tmp_trade_raw'


# ── table setup ───────────────────────────────────────────────────────────────

def ensure_tables(conn, table_name: str, chunk_days: int = 30, compress_after_days: int = 90):
    """Create the trades hypertable and compression policy if they don't exist."""
    index_name = f"{table_name}_symbol_time_idx"

    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {t} (
              time           BIGINT  NOT NULL,
              symbol         TEXT    NOT NULL,
              trade_id       BIGINT  NOT NULL,
              price          DOUBLE PRECISION NOT NULL,
              qty            DOUBLE PRECISION NOT NULL,
              quote_qty      DOUBLE PRECISION NOT NULL,
              is_buyer_maker BOOLEAN NOT NULL,
              is_best_match  BOOLEAN NOT NULL,
              PRIMARY KEY (time, symbol, trade_id)
            );
            """
        ).format(t=sql.Identifier(table_name))
    )

    # time is stored in microseconds; chunk interval in same unit
    chunk_interval_us = chunk_days * 24 * 60 * 60 * 1_000_000
    cur.execute(
        "SELECT create_hypertable(%s::regclass, 'time', chunk_time_interval => %s, if_not_exists => TRUE);",
        (table_name, chunk_interval_us),
    )

    cur.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {t} (symbol, time);").format(
            idx=sql.Identifier(index_name),
            t=sql.Identifier(table_name),
        )
    )

    cur.execute(
        sql.SQL(
            "ALTER TABLE {t} SET ("
            "  timescaledb.compress = true,"
            "  timescaledb.compress_segmentby = 'symbol',"
            "  timescaledb.compress_orderby = 'time ASC'"
            ");"
        ).format(t=sql.Identifier(table_name))
    )

    compress_after_us = compress_after_days * 24 * 60 * 60 * 1_000_000
    cur.execute(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_name = %s AND application_name = 'Compression Policy';",
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
            "ALTER TABLE {t} SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
        ).format(t=sql.Identifier(table_name))
    )
    conn.commit()
    cur.close()


# ── per-connection session setup ──────────────────────────────────────────────

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
                  trade_id_text  TEXT,
                  price          DOUBLE PRECISION,
                  qty            DOUBLE PRECISION,
                  quote_qty      DOUBLE PRECISION,
                  time_text      TEXT,
                  is_buyer_maker TEXT,
                  is_best_match  TEXT
                ) ON COMMIT DELETE ROWS;
                """
            ).format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
        cur.execute(
            sql.SQL("TRUNCATE {raw_table};").format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
    conn.commit()


# ── per-file copy ─────────────────────────────────────────────────────────────

def copy_csv_and_flush(csv_stream, token: str, conn, table_name: str):
    """COPY one trade CSV into the raw temp table, transform and upsert into the hypertable."""
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
                INSERT INTO {t} (time, symbol, trade_id, price, qty, quote_qty, is_buyer_maker, is_best_match)
                SELECT
                  CASE
                    WHEN length(time_text) = 13 THEN time_text::BIGINT * 1000
                    WHEN length(time_text) = 16 THEN time_text::BIGINT
                    ELSE NULL
                  END,
                  %s,
                  trade_id_text::BIGINT,
                  price,
                  qty,
                  quote_qty,
                  is_buyer_maker::BOOLEAN,
                  is_best_match::BOOLEAN
                FROM {raw_table}
                WHERE trade_id_text IS NOT NULL AND time_text IS NOT NULL
                ON CONFLICT (time, symbol, trade_id) DO NOTHING;
                """
            ).format(
                t=sql.Identifier(table_name),
                raw_table=sql.Identifier(TMP_RAW_TABLE),
            ),
            (token,),
        )
        cur.execute(
            sql.SQL("TRUNCATE {raw_table};").format(raw_table=sql.Identifier(TMP_RAW_TABLE))
        )
    conn.commit()


# ── main entry point ──────────────────────────────────────────────────────────

def process_monthly(
    date_range: str,
    folder_path: str,
    market_type: str = 'spot',
    data_type: str = 'trades',
):
    """
    Load all monthly trade zip files for the given parameters into PostgreSQL.

    Table created/targeted: ``{market_type}_trades``
    Note: trades have no freq subdirectory in the Binance archive layout.
    """
    start_date, end_date = parse_date_range_str(date_range)

    table_name = f"{market_type}_trades"
    index_name = f"{table_name}_symbol_time_idx"

    # Trades path: data/{market_type}/monthly/{data_type}/{TOKEN}/{date_range}/
    tokens_dir = os.path.join(folder_path, 'data', market_type, 'monthly', data_type)
    month_work, _, total_files = discover_monthly_work(
        tokens_dir, date_range, start_date, end_date, freq=None
    )

    print(f"Table: {table_name}  |  Discovered {total_files} files to process.")
    if not month_work:
        print("No work found for the given date range and folder path.")
        return

    skip_checksum = os.environ.get('TRADE_SKIP_CHECKSUM', '0').lower() in ('1', 'true', 'yes')
    if skip_checksum:
        print('TRADE_SKIP_CHECKSUM=1 -> skipping checksum verification.')

    # -- Table setup -----------------------------------------------------------
    conn = create_connection(DB_NAME)
    try:
        ensure_tables(conn, table_name)
    except Exception as e:
        print(f"Error ensuring tables: {e}", file=sys.stderr)
        conn.close()
        return
    conn.close()

    # -- Bulk-load optimisation: UNLOGGED + drop secondary index ---------------
    print("Preparing table for bulk load (UNLOGGED, index dropped)...")
    prepare_bulk_load(table_name, index_name)

    # -- Worker pool -----------------------------------------------------------
    num_workers_env = os.environ.get('TRADE_WORKERS')
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

    # -- Post-load: restore durability, rebuild index, compress, analyse -------
    print("\nFinalising bulk load...")
    finalize_bulk_load(table_name, index_name, "symbol, time")

    try:
        with open(os.path.join(folder_path, 'success_marker.txt'), 'a'):
            pass
    except Exception as e:
        print(f"Warning: could not write success marker: {e}", file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) != 5:
        prog = os.path.basename(sys.argv[0])
        print(
            f"Usage: {prog} <DATE_RANGE> <FOLDER_PATH> <MARKET_TYPE> <DATA_TYPE>\n"
            "Example: process_trades.py 2020-01-01_2025-09-30 /data/binance/trade spot trades",
            file=sys.stderr,
        )
        sys.exit(1)

    process_monthly(
        date_range=sys.argv[1],
        folder_path=sys.argv[2],
        market_type=sys.argv[3],
        data_type=sys.argv[4],
    )
