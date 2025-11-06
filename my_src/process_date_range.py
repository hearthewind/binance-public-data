import os
from tqdm import tqdm
import sys
sys.path.append('../')

from my_src.file_utils import verify_checksum, unzip_file
from my_src.sql_connection import create_connection
from my_src.process_kline_file import process_file


def process_monthly(date_range: str, folder_path: str, freq: str='1m', market_type: str='spot', data_type: str='klines'):
    assert market_type in ['spot']
    assert data_type in ['klines', 'trades']

    tokens_dir = os.path.join(folder_path, "data", market_type, "monthly", data_type)
    list_tokens = sorted(os.listdir(tokens_dir))

    sql_connection = create_connection(f"binance_{market_type}_{data_type}_{freq}")

    for token in tqdm(list_tokens, desc="Processing tokens"):
        zip_folder = os.path.join(tokens_dir, token, freq, date_range)
        all_files = os.listdir(zip_folder)
        all_file_names = set([f.split(".")[0] for f in all_files])
        for file_name in all_file_names:
            zip_file_path = os.path.join(zip_folder, f"{file_name}.zip")
            checksum_file_path = os.path.join(zip_folder, f"{file_name}.zip.CHECKSUM")
            # Step 1: Verify checksum
            verify_checksum(zip_file_path, checksum_file_path)
            # Step 2: Unzip file
            csv_file_path = unzip_file(zip_file_path, zip_folder)
            # Step 3: Process CSV file into MySQL
            process_file(csv_file_path, token, sql_connection)
            # Step 4: Remove CSV file after processing
            os.remove(csv_file_path)

    print('Finished, closing sql connection')
    sql_connection.close()

if __name__ == '__main__':
    # date_range = '2020-01-01_2025-09-30'
    # folder_path = "/home/m/data4/Downloads/binance_spot/1min_klines_test"
    # freq = '1m'
    # market_tyoe = 'spot'
    # data_type = 'klines'
    date_range, folder_path, freq, market_tyoe, data_type = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]

    process_monthly(date_range, folder_path, freq, market_tyoe, data_type)