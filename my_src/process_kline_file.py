def process_time(epoch_str):
    epoch = int(epoch_str)
    digits = len(str(epoch))
    if digits == 13:
        epoch *= 1e3
    elif digits == 16:
        pass
    else:
        raise Exception(f'Unexpected epoch digits: {digits}')
    return epoch

def parse_file(file_path):
    klines = []
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 12:
                continue  # Skip lines that don't have enough data

            open_time, close_time = process_time(parts[0]), process_time(parts[6])

            kline = {
                'open_time': open_time,
                'open': float(parts[1]),
                'high': float(parts[2]),
                'low': float(parts[3]),
                'close': float(parts[4]),
                'volume': float(parts[5]),
                'close_time': close_time,
                'quote_volume': float(parts[7]),
                'count': int(parts[8]),
                'taker_buy_base_volume': float(parts[9]),
                'taker_buy_quote_volume': float(parts[10])
            }
            klines.append(kline)
    return klines

def process_file(file_path, pair_name, sql_connection):
    klines = parse_file(file_path)

    cur = sql_connection.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{pair_name}` (
          open_time           BIGINT UNSIGNED NOT NULL,
          open       DOUBLE          NOT NULL,
          high       DOUBLE          NOT NULL,
          low        DOUBLE          NOT NULL,
          close      DOUBLE          NOT NULL,
          volume           DOUBLE          NOT NULL,
          close_time          BIGINT UNSIGNED NOT NULL,
          quote_volume   DOUBLE          NOT NULL,
          count      INT             NOT NULL,
          taker_buy_base_volume  DOUBLE          NOT NULL,
          taker_buy_quote_volume DOUBLE          NOT NULL,
          PRIMARY KEY (open_time),
          KEY idx_open_time (open_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

    insert_query = f"""
        INSERT IGNORE INTO `{pair_name}` (
          open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_base_volume, taker_buy_quote_volume
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    rows = [
        (
            kline['open_time'],
            kline['open'],
            kline['high'],
            kline['low'],
            kline['close'],
            kline['volume'],
            kline['close_time'],
            kline['quote_volume'],
            kline['count'],
            kline['taker_buy_base_volume'],
            kline['taker_buy_quote_volume']
        )
        for kline in klines
    ]

    cur.executemany(insert_query, rows)
    sql_connection.commit()
    cur.close()


if __name__ == "__main__":
    file_path = '/home/m/data4/Downloads/binance_spot/temp_data/LTCBTC-1m-2020-01.csv'
    klines = parse_file(file_path)
    for kline in klines:
        print(kline)
        break

    from my_src.sql_connection import create_connection
    conn = create_connection('binance_spot_klines_1min')
    process_file(file_path, 'TEST_TABLE', conn)
    conn.close()