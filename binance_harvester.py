# Run like this:
# conda activate crypto_bot
# cd C:\Users\decla\OneDrive\coding\bot_code
# python binance_harvester.py -P 7

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
import config
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

NUM_PROCESSES = 10
NUM_QUEUE_ITEMS = 250

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'prices.sqlite3')

PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,description='harvester parameters')
PARSER.add_argument('-T', '--trend_time', help="trend time",required=False)

ARGS = PARSER.parse_args()
predict_time = int(ARGS.trend_time)

pair_file = 'USDT_pairs.txt'
#pair_file = 'USDT_small.txt'
USDT_pairs = open(pair_file).read().splitlines()
stream_pairs = []

minute_seconds = 60
count = 0
count_mins = 0
lateset_timestamp = 0.0

client = Client(config.API_KEY, config.API_SECRET)

def roundTime(dt=None, dateDelta=timedelta(minutes=1)):
	roundTo = dateDelta.total_seconds()
	if dt == None : dt = datetime.now()
	seconds = (dt - dt.min).seconds
	# // is a floor division, not a comment on following line:
	rounding = (seconds+roundTo/2) // roundTo * roundTo
	return dt + timedelta(0,rounding-seconds,-dt.microsecond)

def db_connect(db_path=DEFAULT_PATH):
	conn = sqlite3.connect(db_path,timeout=15)
	return conn
	
conn = db_connect()  # connect to the database

def on_open(ws):
	print('opened connection')

def on_close(ws):
	print('closed connection')

def on_message(queue, ws,message):
	message_dict = json.loads(message)
	candle_closed = message_dict['k']['x'] #candle closed
	if candle_closed:
		#pprint(message_dict)
		#print('*************************************************************')
		queue.put(message_dict['k'])
		
def append_to_table(queue):
	conn = db_connect()
	while True:
		candle = queue.get(True)
		print('.', end='', flush=True)
		#read existing data from database
		old_df = pd.read_sql('SELECT Timestamp,Open,High,Low,Close,Volume FROM ['+candle['s']+']', conn)
		old_df.set_index(['Timestamp'], inplace = True)
		#read new data from candle
		new_data = [int(candle['t']),float(candle['o']),float(candle['h']),float(candle['l']),float(candle['c']),float(candle['v'])]
		#create a new dataframe with the new data added
		df = pd.DataFrame(data=[new_data],columns=['Timestamp','Open','High','Low','Close','Volume'])
		df.set_index(['Timestamp'], inplace = True)
		#print("gets here")
		new_df = pd.concat([old_df[-NUM_QUEUE_ITEMS:],df])
		new_df = old_df.add(df, fill_value=0)
		#recalculate and populate indicators
		new_df["macd"] = macd(new_df['Close'], window_slow = 50, window_fast = 20)
		new_df["macd1226"] = macd(new_df['Close'], window_slow = 26, window_fast = 12)
		new_df["macd_diff"] = macd_diff(new_df['Close'], window_slow = 50, window_fast = 20, window_sign = 26)
		new_df["macd_diff1020"] = macd_diff(new_df['Close'], window_slow = 20, window_fast = 10, window_sign = 50)
		new_df["macd_diff2550"] = macd_diff(new_df['Close'], window_slow = 50, window_fast = 25, window_sign = 50)
		new_df["macd_diff50100"] = macd_diff(new_df['Close'], window_slow = 100, window_fast = 50, window_sign = 50)
		new_df["stoch"] = stoch_signal(new_df['High'],new_df['Low'],new_df['Close'], window = 5, smooth_window = 3)
		new_df["stochrsi"] = stochrsi(new_df['Close'], window = 14, smooth1 = 3, smooth2 = 3)
		new_df["rsi"] = rsi(new_df['Close'], window = 14)
		new_df["sma"] = sma_indicator(new_df['Close'], window = 200)
		new_df["ema"] = ema_indicator(new_df['Close'], window = 200)
		new_df["chaikin_money_flow"] = chaikin_money_flow(new_df['High'],new_df['Low'],new_df['Close'],new_df['Volume'], window = 20)
		new_df["mfi"] = money_flow_index(new_df['High'],new_df['Low'],new_df['Close'],new_df['Volume'], window = 14)
		new_df["trix"] = trix(new_df['Close'], window = 9)
		new_df["tema"] = talib.TEMA(new_df['Close'], timeperiod=15)
		#write new dataframe back to database and replace all existing values
		#pprint(new_df)
		new_df.to_sql(candle['s'], conn, if_exists='replace')

def calculate_top_cryptos():
	global conn, client
	while True:
		try:
			data = []
			for pair in USDT_pairs:
				#get the 1 minute candle from predict time ago
				now_time = roundTime(datetime.utcnow(),timedelta(minutes=1))
				now_stamp = datetime.timestamp(now_time.replace(tzinfo=timezone.utc))
				#print(str(now_stamp))
				predict_stamp = now_stamp - float(predict_time * 60 * 60)
				#print(str(predict_stamp))
				TIME_FORMAT='%Y-%m-%d %H:%M:%S'
				predict_string = datetime.utcfromtimestamp(predict_stamp).strftime(TIME_FORMAT)
				#print(predict_string)
				now_candle = client.get_historical_klines(pair, Client.KLINE_INTERVAL_1MINUTE, "1 minutes ago UTC")
				predict_candle = client.get_historical_klines(pair, Client.KLINE_INTERVAL_1MINUTE, str(predict_string), str(predict_string))
				predict_price = 1.0
				now_price = 1.0
				if len(now_candle) != 0 and len(predict_candle) != 0:
					now_price = float(now_candle[0][4])
					predict_price = float(predict_candle[0][1])
				change = now_price / predict_price
				#pprint(now_candle)
				#pprint(predict_candle)
				#get the most recent price for every crypto
				data.append([pair,change])
			#wait for a minute then recalculate
			data = sorted(data,key=itemgetter(1),reverse=True)
			df = pd.DataFrame(data=data,columns=['Crypto', 'Change'])
			df.to_sql('top_cryptos', conn, if_exists='replace')
			print('done')
			time.sleep(240)
		except Exception as e:
			print("an exception occured - {}".format(e))

def infinity():
    i=0
    while True:
        i+=1
        yield i

def main():
	msg_queue = multiprocessing.Queue()
	cur = conn.cursor()  # instantiate a cursor obj
	for pair in USDT_pairs:
		df = pd.DataFrame(columns=['Timestamp','Open','High','Low','Close','Volume','macd_diff2550','stoch','stochrsi','rsi','chaikin_money_flow','mfi','trix'])
		df.to_sql(pair, conn, if_exists='replace')
		#print("created DB tables")
		#stream_pairs.append(pair.lower() + "@kline_1m")
		stream_pairs.append(pair.lower() + "@kline_5m")
	df = pd.DataFrame(columns=['Crypto', 'Change'])
	df.to_sql('top_cryptos', conn, if_exists='replace')
	pool = multiprocessing.Pool(NUM_PROCESSES, append_to_table,(msg_queue,))
	tc_process = multiprocessing.Process(target=calculate_top_cryptos)
	tc_process.start()
	socket = "wss://stream.binance.com:9443/ws/"+"/".join(stream_pairs)
	print(socket)
	#socket = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
	for i in infinity():
		try:
			ws = websocket.WebSocketApp(socket, on_open=on_open, on_close=on_close)
			ws.on_message = partial(on_message, msg_queue)
			wst = threading.Thread(target=ws.run_forever)
			wst.daemon = True
			wst.start()
			print(str(i) + ": Starting socket session")
			time.sleep(21600)
			print(str(i) + ": Finished socket session")
			ws.keep_running = False
			wst.stop = True
		except Exception as e:
			print("an exception occured - {}".format(e))
	tc_process.terminate()
	tc_process.join()
	#time.sleep(20)
	msg_queue.close()
	msg_queue.join_thread()
	ws.close()

	cur.close()
	conn.close()

if "__main__" == __name__:
	main()