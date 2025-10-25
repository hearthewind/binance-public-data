import os
import hashlib
import zipfile
from tqdm import tqdm
import sys

sys.path.append('../')

from my_src.sql_connection import create_connection
from my_src.process_kline_file import process_file


def verify_checksum(zip_file_path, checksum_file_path):
    """
    Compare SHA256 checksum of zip file with value in checksum file.
    """
    with open(checksum_file_path, 'r') as f:
        expected_checksum = f.read().strip().split('  ')[0]
    sha256 = hashlib.sha256()
    with open(zip_file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    actual_checksum = sha256.hexdigest()
    if actual_checksum != expected_checksum:
        raise Exception(f"Checksum mismatch for {zip_file_path}: expected {expected_checksum}, got {actual_checksum}")
    return True


def unzip_file(zip_file_path, extract_dir):
    """
    Unzip zip_file_path into extract_dir. Returns path to extracted CSV file.
    Assumes the CSV file inside has the same base name as the zip file.
    """
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        # Find the CSV file with the same base name
        base_name = os.path.splitext(os.path.basename(zip_file_path))[0]
        csv_file_path = os.path.join(extract_dir, f"{base_name}.csv")
        if not os.path.exists(csv_file_path):
            raise Exception(f"CSV file {csv_file_path} not found after extracting {zip_file_path}")
        return csv_file_path


def process_monthly(date_range: str, folder_path: str, freq: str='1m', market_type: str='spot', data_type: str='klines'):
    assert market_type in ['spot']
    assert data_type in ['klines', 'trades']

    tokens_dir = os.path.join(folder_path, "data", market_type, "monthly", data_type)
    list_tokens = os.listdir(tokens_dir)

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