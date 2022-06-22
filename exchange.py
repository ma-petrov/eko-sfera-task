from json import loads
from requests import get
from datetime import datetime, timedelta
from pandas import DataFrame, concat
from sqlite3 import connect

from database import DataBaseService

def trunc_day(dt):
    if not isinstance(dt, datetime):
        raise Exception(f'dt is expected to be datetime.datetime, received {type(dt)}')
    return datetime(dt.year, dt.month, dt.day)

class Exchange():
    """
    Operates with echange data, stores in database, updates.
    Do not implements operations with API of specific echange.
    """

    def __init__(self, name, symbol_list):
        self.name = name
        self.symbol_list = symbol_list

    def init(self):
        db = DataBaseService('./marketdata.db')

        data = self.get_data('minute')
        if isinstance(data, DataFrame):
            db.upload_data(data, 'marketdata_minute_candles', is_replace=True)

        data = self.get_data(
            'hour',
            trunc_day(datetime.utcnow()) - timedelta(days=30),
            datetime.utcnow()
        )
        if isinstance(data, DataFrame):
            db.upload_data(data, 'marketdata_hour_candles', is_replace=True)

    def update(self, first_launch_time):
        db = DataBaseService('./marketdata.db')

        query_result = db.load_data_from_query(f"SELECT COALESCE(MAX(dt_timestamp), 0) AS ts FROM marketdata_hour_candles WHERE exchange_name='{self.name}'")
        ts = query_result.ts.loc[0]
        if ts == 0:
            from_ = trunc_day(datetime.fromtimestamp(first_launch_time)) - timedelta(days=30)
        else:
            from_ = datetime.fromtimestamp(ts) + timedelta(hours=1)
        to_ = datetime.utcnow()

        data = self.get_data('hour', from_, to_)
        if isinstance(data, DataFrame):
            db.insert_data(data, 'marketdata_hour_candles')

    def get_data(self, time_frame, from_=None, to_=None):
        data = list()
        for symbol in self.symbol_list:
            symbol_data = self.get_symbol_data(symbol, from_, to_, time_frame)
            if isinstance(symbol_data, DataFrame):
                symbol_data['symbol'] = [symbol] * symbol_data.shape[0]
                symbol_data['exchange_name'] = [self.name] * symbol_data.shape[0]
                data.append(symbol_data)
        if len(data) > 0:
            return concat(data, ignore_index=True)
        
    def get_symbol_data(self, symbol, from_, to_, time_frame):
        """
        Interface of method, that gets data form exchange by API.

        Params:
            symbol - str
            form_ - datetime.datetime
            to_   - datetime.datetime
            time_frame - str, must be 'minute' or 'hour'

        Must returns data in pandas.DataFrame object with columns:
            dt (datetime.datetime), dt_timestamp (timestamp), open_value (float), close_value (float),
            high_value (float), low_value (float), volume (float)
        """
        raise Exception('Method get_symbol_data is not implemented!')


class BitfinexExchange(Exchange):

    def __init__(self, symbol_list):
        super(BitfinexExchange, self).__init__('Bitfinex', symbol_list)

    def get_symbol_data(self, symbol, from_, to_, time_frame):
        if time_frame == 'minute':
            time_frame = '1m'
            limit = 1440
            to_ = trunc_day(datetime.utcnow())
            from_ = trunc_day(datetime.utcnow()) - timedelta(days=1)
        elif time_frame == 'hour':
            if from_ == None:
                raise Exception('from_ is required!')
            if to_ == None:
                to_ = trunc_day(datetime.utcnow())
            limit = int((to_ - from_).total_seconds() / 3600)
            time_frame = '1h'

        start_dt = f'{int(datetime.timestamp(from_)) * 1000}'
        end_dt = f'{int(datetime.timestamp(to_)) * 1000}'
        params = f'limit={limit}&start={start_dt}&end={end_dt}'

        if from_ < to_:
            response = get(f'https://api-pub.bitfinex.com/v2/candles/trade:{time_frame}:t{symbol}/hist/?{params}')
            row_data = loads(response.text)
            try:
                return DataFrame(dict(
                    dt=[datetime.fromtimestamp(int(i[0]) // 1000, tz=None) for i in row_data],
                    dt_timestamp=[int(i[0]) // 1000 for i in row_data],
                    open_value=[i[1] for i in row_data],
                    close_value=[i[2] for i in row_data],
                    high_value=[i[3] for i in row_data],
                    low_value=[i[4] for i in row_data],
                    volume=[i[5] for i in row_data],
                ))
            except IndexError:
                print(row_data)


class KrakenExchange(Exchange):

    def __init__(self, symbol_list):
        super(KrakenExchange, self).__init__('Kraken', symbol_list)

    def get_symbol_data(self, symbol, from_, to_, time_frame):
        td = dict(minute=timedelta(hours=12), hour=timedelta(days=30))[time_frame]
        from_ = from_ or datetime.now() - td
        to_ = datetime.now()

        if from_ < to_:
            since = datetime.timestamp(from_)
            interval = dict(minute='1', hour='60')[time_frame]
            params = f'pair={symbol}&interval={interval}&since={since}'
            response = get(f'https://api.kraken.com/0/public/OHLC?{params}')
            response_obj = loads(response.text)
            try:
                row_data = loads(response.text)['result']
                row_data = row_data[list(row_data.keys())[0]]
                data = DataFrame(dict(
                    dt=[datetime.fromtimestamp(i[0], tz=None) for i in row_data],
                    dt_timestamp=[i[0] for i in row_data],
                    open_value=[i[1] for i in row_data],
                    close_value=[i[4] for i in row_data],
                    high_value=[i[2] for i in row_data],
                    low_value=[i[3] for i in row_data],
                    volume=[i[6] for i in row_data],
                ))
                return data
            except KeyError as e:
                print(f'Error when parsing JSON, {e}')
