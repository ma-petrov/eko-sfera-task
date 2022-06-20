from datetime import datetime
from sqlite3 import connect

class Casting():

    def cast_datetime(dt):
        return f"'{datetime.strftime(dt, r'%Y-%m-%d %H:%M')}'"

    def cast_str(s):
        return f"'{s}'"

    def cast_bool(b):
        return 'TRUE' if b else 'FALSE'
    
    SERIALIZE_FUNC = {
        "<class 'int'>": str,
        "int64": str,
        "<class 'float'>": str,
        "float64": str,
        "<class 'datetime.datetime'>": cast_datetime,
        "datetime.datetime": cast_datetime,
        "datetime64[ns, UTC]": cast_datetime,
        "datetime64[ns]": cast_datetime,
        "<class 'bool'>": cast_bool,
        "bool": cast_bool,
        "<class 'str'>": cast_str,
        "object": cast_str
    }

    TYPES = {
        "<class 'int'>": 'INTEGER',
        "int64": 'INTEGER',
        "<class 'float'>": 'REAL',
        "float64": "REAL",
        "<class 'datetime.datetime'>": 'TEXT',
        "datetime.datetime": "TEXT",
        "datetime64[ns, UTC]": "TEXT",
        "datetime64[ns]": "TEXT",
        "<class 'bool'>": 'TEXT',
        "bool": 'TEXT',
        "<class 'str'>": 'TEXT',
        "object": 'TEXT'
    }


class DataBaseService():
    def __init__(self, database):
        self.conn = connect(database)

    def _values_row(self, serialize_func, row):
        return f"({', '.join([f(x) for f, x in zip(serialize_func, row)])})"

    def generate_insert_query(self, data, table_name):
        cols = ', '.join(list(data.columns))
        serialize_func = [Casting.SERIALIZE_FUNC[str(t)] for t in data.dtypes]
        values = ', '.join([self._values_row(serialize_func, row) for _, row in data.iterrows()])
        return f'INSERT INTO {table_name} ({cols}) VALUES {values}'

    def generate_create_query(self, columns, dtypes, table_name):
        types = [Casting.TYPES[str(t)] for t in dtypes]
        cols = ', '.join([f'{c} {t}' for c, t in zip(columns, types)])
        return f'CREATE TABLE {table_name} ({cols})'

    def insert_data(self, data, table_name):
        cursor = self.conn.cursor()
        cursor.execute(generate_insert_query(data, table_name))
        self.conn.commit()

    def upload_data(self, data, table_name, is_replace=False):
        cursor = self.conn.cursor()
        if is_replace:
            try:
                cursor.execute(f'DROP TABLE {table_name}')
            except:
                pass
        cursor.execute(self.generate_create_query(data.columns, data.dtypes, table_name))
        cursor.execute(self.generate_insert_query(data, table_name))
        self.conn.commit()

    def load_data(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT * FROM {table_name}')
        data = {col.name: list() for col in cursor.description}
        for row in cursor.fetchall():
            for col, value in zip(data.keys(), row):
                data[col].append(value)
        return DataFrame(data)

    def load_data_from_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        data = {col[0]: list() for col in cursor.description}
        for row in cursor.fetchall():
            for col, value in zip(data.keys(), row):
                data[col].append(value)
        return DataFrame(data)

