from pandas import DataFrame
from datetime import datetime
from unittest import main, TestCase

from database import Casting, DataBaseService

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

if __name__ == '__main__':
    main()