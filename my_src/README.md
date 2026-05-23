# ETL — my_src

Custom ETL scripts that load Binance public data CSV archives into
PostgreSQL / TimescaleDB.

## Setup

```bash
cp ../.env.example ../.env   # then fill in your passwords
pip install psycopg2-binary python-dotenv tqdm
```

`.env` variables (see `.env.example` for the full list):

| Variable | Description |
|----------|-------------|
| `PGHOST` | PostgreSQL host (default `localhost`) |
| `PGSUPERUSER` / `PG_SUPERUSER_PASSWORD` | Superuser — only used to create the DB and role |
| `APP_DB_USER` / `APP_DB_PASSWORD` | Application role — used for all data writes |

---

## Loading klines

```bash
python process_kline_v2.py <DATE_RANGE> <DATA_ROOT> <FREQ> <MARKET_TYPE> <DATA_TYPE>

# Examples
python process_kline_v2.py 2024-01-01_2024-02-01 /data/binance 1m spot klines
python process_kline_v2.py 2024-01-01_2024-02-01 /data/binance 1h futures/um klines
```

Table name created: `{market_type}_klines_{freq}` — e.g. `spot_klines_1m`.

**Bulk-load strategy** for speed:
1. `ALTER TABLE SET UNLOGGED` — skips WAL during load (3–5× speedup)
2. Parallel workers (configurable via `KLINE_WORKERS` env var)
3. Per-connection temp table + `COPY` → transform → `INSERT ON CONFLICT DO NOTHING`
4. After all workers finish: `SET LOGGED` → rebuild index → compress chunks → `VACUUM ANALYZE`

## Loading symbol metadata

```bash
python save_metadata.py spot          # → binance_metadata.spot_symbols
python save_metadata.py futures_um    # → binance_metadata.futures_um_symbols
python save_metadata.py futures_cm    # → binance_metadata.futures_cm_symbols
```

## Database schema

### Klines hypertable

```sql
CREATE TABLE spot_klines_1m (
  open_time   BIGINT NOT NULL,   -- epoch microseconds (16 digits)
  symbol      TEXT   NOT NULL,
  open        DOUBLE PRECISION NOT NULL,
  high        DOUBLE PRECISION NOT NULL,
  low         DOUBLE PRECISION NOT NULL,
  close       DOUBLE PRECISION NOT NULL,
  volume      DOUBLE PRECISION NOT NULL,
  close_time  BIGINT NOT NULL,
  quote_volume          DOUBLE PRECISION NOT NULL,
  count                 BIGINT NOT NULL,
  taker_buy_base_volume  DOUBLE PRECISION NOT NULL,
  taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (open_time, symbol)
);
-- TimescaleDB hypertable, 30-day chunks, compressed after 90 days
-- Index: (symbol, open_time) for fast per-symbol range scans
```

### Symbols table

```sql
CREATE TABLE spot_symbols (
  symbol      TEXT PRIMARY KEY,
  base_asset  TEXT NOT NULL,
  quote_asset TEXT NOT NULL
);
```
