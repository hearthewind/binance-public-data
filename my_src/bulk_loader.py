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


def parse_base_month(base: str) -> date | None:
    parts = base.split('-')
    if len(parts) < 4:
        return None
    try:
        return date(int(parts[-2]), int(parts[-1]), 1)
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
    tokens_dir: str,
    date_range: str,
    start_month: date,
    end_month: date,
    freq: str | None = None,
) -> tuple[dict[date, list[FileTask]], dict[str, int], int]:
    month_work: dict[date, list[FileTask]] = defaultdict(list)
    token_file_counts: dict[str, int] = defaultdict(int)
    total_files = 0

    if not os.path.isdir(tokens_dir):
        raise FileNotFoundError(
            f"tokens directory not found: {tokens_dir!r}\n"
            "Check that DATA_ROOT and MARKET_TYPE are correct."
        )
    tokens = sorted(os.listdir(tokens_dir))
    for token in tokens:
        if freq is not None:
            zip_folder = os.path.join(tokens_dir, token, freq, date_range)
        else:
            zip_folder = os.path.join(tokens_dir, token, date_range)
        if not os.path.isdir(zip_folder):
            continue
        base_names = sorted({f.split('.')[0] for f in os.listdir(zip_folder)})
        for base in base_names:
            month_marker = parse_base_month(base)
            if month_marker is None or month_marker < start_month or month_marker > end_month:
                continue
            zip_path = os.path.join(zip_folder, f"{base}.zip")
            checksum_path = os.path.join(zip_folder, f"{base}.zip.CHECKSUM")
            if not (os.path.exists(zip_path) and os.path.exists(checksum_path)):
                continue
            month_work[month_marker].append(FileTask(token, zip_path, checksum_path, zip_folder))
            token_file_counts[token] += 1
            total_files += 1

    return month_work, token_file_counts, total_files


def prepare_bulk_load(table_name: str, index_name: str) -> None:
    conn = create_connection(DB_NAME)
    try:
        with tqdm(total=2, desc=f"Preparing {table_name}", unit="step", leave=True) as pbar:
            with conn.cursor() as cur:
                pbar.set_postfix_str("SET UNLOGGED")
                cur.execute(
                    sql.SQL("ALTER TABLE {t} SET UNLOGGED;").format(t=sql.Identifier(table_name))
                )
                pbar.update(1)
                pbar.set_postfix_str("DROP INDEX")
                cur.execute(
                    sql.SQL("DROP INDEX IF EXISTS {idx};").format(idx=sql.Identifier(index_name))
                )
                pbar.update(1)
        conn.commit()
    finally:
        conn.close()


def finalize_bulk_load(table_name: str, index_name: str, index_columns: str) -> None:
    """Restore durability, rebuild the secondary index, compress all chunks, re-enable autovacuum."""
    conn = create_connection(DB_NAME)
    try:
        conn.autocommit = True
        with tqdm(total=5, desc=f"Finalising {table_name}", unit="step", leave=True) as pbar:
            with conn.cursor() as cur:
                pbar.set_postfix_str("SET LOGGED")
                cur.execute(
                    sql.SQL("ALTER TABLE {t} SET LOGGED;").format(t=sql.Identifier(table_name))
                )
            pbar.update(1)

            with conn.cursor() as cur:
                pbar.set_postfix_str("rebuild index")
                cur.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {t} ({cols});").format(
                        idx=sql.Identifier(index_name),
                        t=sql.Identifier(table_name),
                        cols=sql.SQL(index_columns),
                    )
                )
            pbar.update(1)

            with conn.cursor() as cur:
                pbar.set_postfix_str("compress chunks")
                cur.execute(
                    "SELECT compress_chunk(c, if_not_compressed => TRUE) FROM show_chunks(%s) AS c;",
                    (table_name,),
                )
            pbar.update(1)

            with conn.cursor() as cur:
                pbar.set_postfix_str("autovacuum on")
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} SET (autovacuum_enabled = true, toast.autovacuum_enabled = true);"
                    ).format(t=sql.Identifier(table_name))
                )
            pbar.update(1)

            with conn.cursor() as cur:
                pbar.set_postfix_str("VACUUM ANALYZE")
                cur.execute(sql.SQL("VACUUM ANALYZE {t};").format(t=sql.Identifier(table_name)))
            pbar.update(1)
    finally:
        conn.close()


def run_worker_pool(
    month_work: dict[date, list[FileTask]],
    total_files: int,
    db_name: str,
    skip_checksum: bool,
    num_workers: int,
    setup_session_fn,
    process_file_fn,
) -> list[str]:
    """
    setup_session_fn(conn) -> None       called once per worker thread on its connection
    process_file_fn(conn, csv_stream, token: str) -> None   called per file
    Returns list of error messages (empty on full success).
    """
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
        conn = create_connection(db_name)
        try:
            setup_session_fn(conn)
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
                        process_file_fn(conn, csv_stream, task.token)
                except Exception as exc:
                    msg = f"Worker {worker_id} failed for {task.token} ({month_str}): {exc}"
                    print(msg, file=sys.stderr)
                    with error_lock:
                        errors.append(msg)
                finally:
                    with progress_lock:
                        worker_stats[worker_id] += 1
                        worker_last_active[worker_id] = time.monotonic()
                        busiest = max(worker_last_active, key=worker_last_active.get)
                        pbar.set_postfix({
                            "last": worker_id,
                            "busy": busiest,
                            "queue": task_queue.qsize(),
                        })
                        pbar.update(1)
                    task_queue.task_done()
        finally:
            conn.close()

    with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
        workers: list[Thread] = []
        for idx in range(num_workers):
            t = Thread(target=worker_main, args=(idx + 1, pbar), daemon=True)
            t.start()
            workers.append(t)
        for _ in workers:
            task_queue.put(None)
        task_queue.join()
        for t in workers:
            t.join()

    return errors
