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
        db.upload_data(data, 'marketdata_minute_candles', is_replace=True)

        data = self.get_data(
            'hour',
            trunc_day(datetime.utcnow()) - timedelta(days=30),
            datetime.utcnow()
        )
        db.upload_data(data, 'marketdata_hour_candles', is_replace=True)

    def update(self, first_launch_time):
        db = DataBaseService('./marketdata.db')

        # тк в sqlite не реализована работа с датами, выгружаются все даты свеч по котировкам биржи, последняя дата ищется в питоне.
        dt_data = db.load_data_from_query(f"SELECT dt FROM marketdata_hour_candles WHERE exchange_name='{self.name}'")
        if dt_data.shape[0] == 0:
            from_ = trunc_day(datetime.fromtimestamp(first_launch_time)) - timedelta(days=30)
        else:
            dt_data['dt_'] = dt_data['dt'].apply(lambda x: datetime.strptime(x, r'%Y-%m-%d %H:%M'))
            from_ = dt_data.dt_.max() + timedelta(hours=1)
        to_ = datetime.utcnow()

        data = self.get_data('hour', from_, to_)
        db.insert_data(data, 'marketdata_hour_candles')

    def get_data(self, time_frame, from_=None, to_=None):
        data = list()
        for symbol in self.symbol_list:
            symbol_data = self.get_symbol_data(symbol, from_, to_, time_frame)
            symbol_data['symbol'] = [symbol] * symbol_data.shape[0]
            symbol_data['exchange_name'] = [self.name] * symbol_data.shape[0]
            data.append(symbol_data)
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
            symbol (str), dt (datetime.datetime), open_value (float), close_value (float),
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

        response = get(f'https://api-pub.bitfinex.com/v2/candles/trade:{time_frame}:t{symbol}/hist/?{params}')
        row_data = loads(response.text)

        return DataFrame(dict(
            dt=[datetime.fromtimestamp(i[0]/1000, tz=None) for i in row_data],
            open_value=[i[1] for i in row_data],
            close_value=[i[2] for i in row_data],
            high_value=[i[3] for i in row_data],
            low_value=[i[4] for i in row_data],
            volume=[i[5] for i in row_data],
        ))


class KrakenExchange(Exchange):

    def __init__(self, symbol_list):
        super(KrakenExchange, self).__init__('Kraken', symbol_list)

    def get_symbol_data(self, symbol, from_, to_, time_frame):
        def get_batch_params(from_, to_, time_frame):
            batch_len = 720
            
            if time_frame == 'minute':
                time_frame_ = 60
                def get_time_delta(x):
                    return timedelta(minutes=batch_len*x)
            elif time_frame == 'hour':
                time_frame_ = 3600
                def get_time_delta(x):
                    return timedelta(hours=batch_len*x)
            else:
                return
            
            limit = int((to_ - from_).total_seconds() / time_frame_)
            batch_sizes = [batch_len] * (limit // batch_len) + [limit % batch_len]
            if batch_sizes[-1] == 0:
                batch_sizes = batch_sizes[:-1]
            since = [from_ + get_time_delta(i) for i in range(len(batch_sizes))]
            since = [f'{int(datetime.timestamp(since_))}' for since_ in since]
            return since, batch_sizes
        
        def get_batch(symbol, interval, since, size):
            params = f'pair={symbol}&interval={interval}&since={since}'
            response = get(f'https://api.kraken.com/0/public/OHLC?{params}')
            row_data = loads(response.text)['result']
            row_data = row_data[list(row_data.keys())[0]]
            data = DataFrame(dict(
                dt=[datetime.fromtimestamp(i[0], tz=None) for i in row_data],
                open_value=[i[1] for i in row_data],
                close_value=[i[4] for i in row_data],
                high_value=[i[2] for i in row_data],
                low_value=[i[3] for i in row_data],
                volume=[i[6] for i in row_data],
            ))
            return data.sort_values(by=['dt'], ignore_index=True).loc[:size-1]
        
        if time_frame == 'minute':
            interval = '1'
            from_ = trunc_day(datetime.utcnow()) - timedelta(days=1)
            to_ = trunc_day(datetime.utcnow())
        elif time_frame == 'hour':
            interval = '60'
            if from_ == None:
                raise Exception('from_ is required!')
            if to_ == None:
                to_ = trunc_day(datetime.utcnow())
        else:
            raise Exception(f'time_frame is expected to be on of ["minute", "hour"], recieved {time_frame}')

        batches = list()
        since, batch_sizes = get_batch_params(from_, to_, time_frame)
        for s, b in zip(since, batch_sizes):
            batches.append(get_batch(symbol, interval, s, b))
        data = concat(batches, ignore_index=True)
        return data


    
