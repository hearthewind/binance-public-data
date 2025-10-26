import urllib.request
import json

from my_src.sql_connection import create_connection

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

    sql_connection = create_connection(f"binance_metadata")
    cur = sql_connection.cursor()

    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `spot_symbols` (
              symbol VARCHAR(32) NOT NULL,
              base_asset VARCHAR(32) NOT NULL,
              quote_asset VARCHAR(32) NOT NULL,
              PRIMARY KEY (symbol),
              KEY idx_symbol (symbol)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

    insert_query = f"""
            INSERT IGNORE INTO `spot_symbols` (
              symbol, base_asset, quote_asset
            ) VALUES (%s, %s, %s);
            """

    cur.executemany(insert_query, symbol2pair_list)
    sql_connection.commit()
    cur.close()
    sql_connection.close()

if __name__ == "__main__":
    save_metadata()