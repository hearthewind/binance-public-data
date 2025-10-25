#!/bin/bash

python ../python/download-kline.py -i 1m -startDate 2020-01-01 -endDate 2025-09-30 \
-folder ~/data4/Downloads/binance_spot/1min_klines/ -c 1 -t spot -skip-daily 1