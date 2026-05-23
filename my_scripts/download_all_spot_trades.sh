#!/bin/bash

python ../python/download-trade.py -startDate 2020-01-01 -endDate 2025-09-30 \
-folder ~/data4/Downloads/binance_spot/trade/ -c 1 -t spot -skip-daily 1