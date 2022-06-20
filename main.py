from json import dumps, loads
from datetime import datetime

from sqlite3 import connect
from pandas import DataFrame

from exchange import BitfinexExchange, KrakenExchange
# from .server import Server

class MarketData:

    def __init__(self, exchanges):
        self.exchanges = exchanges

    def init(self):
        for exchange in self.exchanges:
            exchange.init()

    def update(self):
        for exchange in self.exchanges:
            exchange.update()

def get_meta():
    try:
        with open('./.meta', 'r') as f:
            return loads(f.read())
    except FileNotFoundError:
        first_launch_time = datetime.timestamp(datetime.utcnow())
        # with open('./.meta', 'w') as f:
        #     f.write(dumps(dict(is_first_launch=False, first_launch_time=first_launch_time)))
        return dict(is_first_launch=True, first_launch_time=first_launch_time)

if __name__ == '__main__':
    marketdata = MarketData([
        BitfinexExchange(['tBTCUSD', 'tETHUSD', 'tXRPEUR', 'tXRPUSD']),
        # KrakenExchange(['tBTCUSD', 'tETHUSD', 'tXRPEUR', 'tXRPUSD'])
    ])

    meta = get_meta()

    if meta['is_first_launch']:
        marketdata.init()
    else:
        marketdata.update()

    conn = connect('./marketdata.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM marketdata_hour_candles')
    data = {col[0]: list() for col in cursor.description}
    for row in cursor.fetchall():
        for col, value in zip(data.keys(), row):
            data[col].append(value)
    DataFrame(data).to_excel('hour.xlsx')

    cursor.execute('SELECT * FROM marketdata_minute_candles')
    data = {col[0]: list() for col in cursor.description}
    for row in cursor.fetchall():
        for col, value in zip(data.keys(), row):
            data[col].append(value)
    DataFrame(data).to_excel('minute.xlsx')

    # Server().run(port=6969)
