import urllib.request
import json

from my_src.sql_connection import create_connection, ensure_database_ready

spot_url = "https://api.binance.com/api/v3/exchangeInfo"
cm_url = "https://dapi.binance.com/dapi/v1/exchangeInfo"
um_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

def symbol2pair(response):
    ret = []
    for item in response['symbols']:
        symbol = item['symbol']
        base = item['baseAsset']
        quote = item['quoteAsset']
        ret.append((symbol, base, quote))

    ret = sorted(ret, key=lambda x: x[0])
    return ret

def save_metadata():
    response = urllib.request.urlopen(spot_url).read()
    response = json.loads(response)
    symbol2pair_list = symbol2pair(response)

    ensure_database_ready("binance_metadata", install_timescaledb=False)
    sql_connection = create_connection("binance_metadata")
    cur = sql_connection.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spot_symbols (
          symbol TEXT PRIMARY KEY,
          base_asset TEXT NOT NULL,
          quote_asset TEXT NOT NULL
        );
        """
    )

    cur.executemany(
        """
        INSERT INTO spot_symbols(symbol, base_asset, quote_asset)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE
        SET base_asset = EXCLUDED.base_asset,
            quote_asset = EXCLUDED.quote_asset;
        """,
        symbol2pair_list,
    )
    sql_connection.commit()
    cur.close()
    sql_connection.close()

if __name__ == "__main__":
    save_metadata()
