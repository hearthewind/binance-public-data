import os
import hashlib
import zipfile
import sys

sys.path.append('../')


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