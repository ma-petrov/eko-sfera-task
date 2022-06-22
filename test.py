from pandas import DataFrame
from datetime import datetime
from unittest import main, TestCase

from database import Casting, DataBaseService
from main import MarketData

JSON_RESPONSE = '[{"Type": "min", "time": "2022-05-24 02:00", "close": 29070.0, "open": 29185.7, "high": 29185.7, "low": 28829.5, "volume": 173.85453724, "Exchange": "Bitfinex"}, {"Type": "max", "time": "2022-05-24 01:00", "close": 29184.5, "open": 29310.3, "high": 29310.3, "low": 29013.4, "volume": 119.97294608, "Exchange": "Kraken"}, {"Type": "max", "time": "2022-05-25 00:00", "close": 29117.2, "open": 29070.0, "high": 29227.7, "low": 29049.0, "volume": 40.64422544, "Exchange": "Kraken"}]'

CREATE_TABLE = 'CREATE TABLE marketdata_hour_candles (dt TEXT, dt_timestamp INTEGER, open_value TEXT, close_value TEXT, high_value TEXT, low_value TEXT, volume TEXT, symbol TEXT, exchange_name TEXT)'

class TestMarketDataMethods(TestCase):

    def test_get_min_max_candles(self):
        data = DataFrame(dict(
            dt=['2022-05-24 00:00', '2022-05-24 01:00', '2022-05-24 02:00', '2022-05-24 03:00', '2022-05-25 00:00'],
            open_value=[29316.9, 29310.3, 29185.7, 29070.0, 29070.0],
            close_value=[29326.8, 29184.5, 29070.0, 29117.2, 29117.2],
            high_value=[30445.3, 29310.3, 29185.7, 29227.7, 29227.7],
            low_value=[29272.4, 29013.4, 28829.5, 29049.0, 29049.0],
            volume=[263.20838291, 119.97294608, 173.85453724, 40.64422544, 40.64422544],
            symbol=['XRPEUR', 'BTCUSD', 'BTCUSD', 'BTCUSD', 'BTCUSD'],
            exchange_name=['Kraken', 'Kraken', 'Bitfinex', 'Kraken', 'Kraken']
        ))
        db = DataBaseService('./.test_db.db')
        db.upload_data(data, 'marketdata_hour_candles', is_replace=True)
        self.assertEqual(MarketData.get_min_max_candles(db, 'BTCUSD'), JSON_RESPONSE)

class TestCastingMethods(TestCase):

    def test_cast_datetime(self):
        self.assertEqual(Casting.cast_datetime(datetime(1999, 2, 22, 4, 30)), "'1999-02-22 04:30'")


class TestDataBaseServiceMethods(TestCase):

    def test_generate_create_query(self):
        data = DataFrame(dict(text_data='some text data', time_data=datetime(1999, 2, 22, 4, 30), float_num=13.312), index=[0])
        table_name = 'simple_new_table'
        db = DataBaseService('./.test_db.db')
        self.assertEqual(
            db.generate_create_query(data.columns, data.dtypes, table_name),
            "CREATE TABLE simple_new_table (text_data TEXT, time_data TEXT, float_num REAL)"
        )

    def test_generate_insert_query(self):
        data = DataFrame(dict(text_data='some text data', time_data=datetime(1999, 2, 22, 4, 30), float_num=13.312), index=[0])
        table_name = 'simple_new_table'
        db = DataBaseService('./.test_db.db')
        self.assertEqual(
            db.generate_insert_query(data, table_name),
            "INSERT INTO simple_new_table (text_data, time_data, float_num) VALUES ('some text data', '1999-02-22 04:30', 13.312)"
        )

    def test_create_table(self):
        table_name = 'marketdata_hour_candles'
        columns_types = dict(
            dt='TEXT',
            dt_timestamp='INTEGER',
            open_value='TEXT',
            close_value='TEXT',
            high_value='TEXT',
            low_value='TEXT',
            volume='TEXT',
            symbol='TEXT',
            exchange_name='TEXT'
        )
        db = DataBaseService('./.test_db.db')
        self.assertEqual(db.create_table(table_name, columns_types, is_replace=True), CREATE_TABLE)

if __name__ == '__main__':
    main()