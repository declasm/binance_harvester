# binance_harvester
> A Python 3 script to harvest data from the Binance socket stream and calculate popular TA indicators and produce lists of top trending coins storing data in an SQLite3 database for use by algorithmic and bot traders 

The script will populate an SQLite3 database called prices.sqlite3 in the same directory as the script. 

By default the script will populate the DB with data from 5 minute candles and takes about 14 seconds from the reception of candle data on the socket to publish the data for every crypto pair and indicator in the tables ready for your algos or bots

The top trending coins list is stored in a table called "top_cryptos"

Other data is stored in tables named for the coin pair eg "BTCUSDT"

coin pairs used are listed in the text file USDT_pairs.txt

The fields of recorded data and indicators are as follows:
Timestamp,Open,High,Low,Close,Volume,macd,macd1226,macd_diff,macd_diff1020,macd_diff2550,macd_diff50100,stoch,stochrsi,rsi,sma,ema,chaikin_money_flow,mfi,trix,tema


## Dependencies

```python
import websocket, json, pprint
from sqlite3 import connect
import pandas as pd
import sqlite3
import argparse
import time
import math
import os
from pprint import pprint
from datetime import datetime, timedelta, timezone
import csv
from collections import deque
import copy
from binance.client import Client
from binance.enums import *
from ta import add_all_ta_features
from ta.utils import dropna
from ta.trend import macd, macd_diff, sma_indicator, ema_indicator, trix
from ta.momentum import stoch, stochrsi, stoch_signal, rsi
from ta.volume import chaikin_money_flow, money_flow_index
from functools import partial
import multiprocessing
import threading
import os
import time
from operator import itemgetter
import talib
```

You must also create a file called config.py in the same directory as the script and populate it with your Binance API keys following the format of SAMPLE_config.py

## Usage

```bash
python binance_harvester -T 5
```

Ideally run in the background:

```bash
nohup python binance_harvester.py -T 5 &
```


Where the T parameter specifies number of hours for calculating the list of top trending coins eg using "1" will give you the top trending coins over the last 1 hour.

The top trending coins list is stored in a table called "top_cryptos"

Other data is stored in tables named for the coin pair eg "BTCUSDT"

coin pairs used are listed in the text file USDT_pairs.txt

