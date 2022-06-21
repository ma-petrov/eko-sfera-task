from json import dumps, loads
from datetime import datetime
from aiohttp import web
from database import DataBaseService

from exchange import BitfinexExchange, KrakenExchange

class MarketData():

    def __init__(self, exchanges):
        self.exchanges = exchanges

    def init(self):
        for exchange in self.exchanges:
            exchange.init()

    def update(self, first_launch_time):
        for exchange in self.exchanges:
            exchange.update(first_launch_time)

    @classmethod
    def get_min_max_candles(cls, db, symbol):
        query = f"""
        SELECT *
        FROM (
            SELECT
                t.*,
                ROW_NUMBER() OVER (PARTITION BY dt_day ORDER BY low_value) AS rn_low_min,
                ROW_NUMBER() OVER (PARTITION BY dt_day ORDER BY high_value DESC) AS rn_high_max
            FROM (
                SELECT t.*, SUBSTR(dt, 1, 10) AS dt_day
                FROM marketdata_hour_candles t
                WHERE symbol='{symbol}'
            ) t
        )
        WHERE rn_low_min=1 OR rn_high_max=1
        """

        data = db.load_data_from_query(query)

        if data.shape[0] > 0:
            response_obj = list()
            for idx, row in data.iterrows():
                response_obj.append({
                    'Type': 'max' if row.rn_high_max == 1 else 'min',
                    'time': row['dt'],
                    'close': row.close_value,
                    'open': row.open_value,
                    'high': row.high_value,
                    'low': row.low_value,
                    'volume': row.volume,
                    'Exchange': row.exchange_name
                })
            return dumps(response_obj)
        else:
            return dumps(dict(result='no candles data for specified symbol'))
        

def get_meta():
    try:
        with open('./.meta', 'r') as f:
            return loads(f.read())
    except FileNotFoundError:
        first_launch_time = datetime.timestamp(datetime.utcnow())
        with open('./.meta', 'w') as f:
            f.write(dumps(dict(is_first_launch=False, first_launch_time=first_launch_time)))
        return dict(is_first_launch=True, first_launch_time=first_launch_time)

if __name__ == '__main__':
    marketdata = MarketData([
        BitfinexExchange(['BTCUSD', 'ETHUSD', 'XRPEUR', 'XRPUSD']),
        KrakenExchange(['BTCUSD', 'ETHUSD', 'XRPEUR', 'XRPUSD'])
    ])

    meta = get_meta()

    if meta['is_first_launch']:
        marketdata.init()
    else:
        marketdata.update(meta['first_launch_time'])

    async def get_min_max_candles(request):
        symbol = request.match_info.get('symbol', 'NULL')
        if symbol == 'NULL':
            text = dumps(dict(error='symbol param is required'))
        else:
            db = DataBaseService('./marketdata.db')
            text = MarketData.get_min_max_candles(db, symbol)
        return web.Response(text=text)

    async def handler(request):
        return web.Response(text='it works!')

    app = web.Application()
    app.add_routes([
        web.get(r'/api/get-min-max-candles/', get_min_max_candles),
        web.get(r'/api/get-min-max-candles/{symbol}', get_min_max_candles)
    ])
    web.run_app(app)