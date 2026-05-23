import sys
import urllib.request
import json

from psycopg2 import sql

from my_src.sql_connection import create_connection, ensure_database_ready


# ── Market type registry ───────────────────────────────────────────────────────

MARKET_CONFIGS: dict[str, dict] = {
    'spot': {
        'url': 'https://api.binance.com/api/v3/exchangeInfo',
        'table': 'spot_symbols',
    },
    'futures_um': {
        'url': 'https://fapi.binance.com/fapi/v1/exchangeInfo',
        'table': 'futures_um_symbols',
    },
    'futures_cm': {
        'url': 'https://dapi.binance.com/dapi/v1/exchangeInfo',
        'table': 'futures_cm_symbols',
    },
}


def symbol2pair(response: dict) -> list[tuple[str, str, str]]:
    ret = [
        (item['symbol'], item['baseAsset'], item['quoteAsset'])
        for item in response['symbols']
    ]
    return sorted(ret, key=lambda x: x[0])


def save_metadata(market_type: str = 'spot') -> None:
    """Fetch exchange info for `market_type` and upsert into the metadata table.

    Supported market types: 'spot', 'futures_um', 'futures_cm'.
    """
    if market_type not in MARKET_CONFIGS:
        raise ValueError(
            f"Unknown market_type {market_type!r}. "
            f"Choose from: {list(MARKET_CONFIGS)}"
        )

    config = MARKET_CONFIGS[market_type]
    table_name = config['table']

    response = urllib.request.urlopen(config['url']).read()
    symbol2pair_list = symbol2pair(json.loads(response))

    ensure_database_ready("binance_metadata", install_timescaledb=False)
    conn = create_connection("binance_metadata")
    try:
        cur = conn.cursor()

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {table} (
                  symbol      TEXT PRIMARY KEY,
                  base_asset  TEXT NOT NULL,
                  quote_asset TEXT NOT NULL
                );
                """
            ).format(table=sql.Identifier(table_name))
        )

        cur.executemany(
            sql.SQL(
                """
                INSERT INTO {table}(symbol, base_asset, quote_asset)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE
                  SET base_asset  = EXCLUDED.base_asset,
                      quote_asset = EXCLUDED.quote_asset;
                """
            ).format(table=sql.Identifier(table_name)).as_string(conn),
            symbol2pair_list,
        )

        conn.commit()
        print(f"Saved {len(symbol2pair_list)} symbols to {table_name}.")
    finally:
        conn.close()


if __name__ == "__main__":
    _market_type = sys.argv[1] if len(sys.argv) > 1 else 'spot'
    if _market_type not in MARKET_CONFIGS:
        print(
            f"Unknown market_type {_market_type!r}. "
            f"Choose from: {list(MARKET_CONFIGS)}",
            file=sys.stderr,
        )
        sys.exit(1)
    save_metadata(_market_type)
