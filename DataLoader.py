import pandas as pd
from datetime import date, datetime,timedelta
from pathlib import Path
import pdb

def get_csv_candle_loader(candle_type, path, start=datetime(2013,1,1,0,0,0), end=date.today(),converter = None):
    return CsvCandleLoader(candle_type, path, start=start, end=end, converter=converter)

def get_oanda_candle_loader(symbol, account_id, period, time_delta, ctx):
    return OandaCandleLoader(symbol, account_id, period, time_delta, ctx)

def get_oanda_tick_loader(account_id, symbol, ctx):
    return OandaTickLoader(account_id, symbol, ctx)

class DataLoader:
    def __init__(self, candle_type, start=datetime(2013,1,1,0,0,0), end=date.today()):
         self.candle_type = candle_type
         self.start = start
         self.end = end
    
    def loadData(self):
        return []
    
class Candle:
    def __init__(self, **kwargs):
        self.time = kwargs.get("time")
        self.bid = kwargs.get("bid")
        self.ask = kwargs.get("ask")
        self.mid = kwargs.get("mid")
        self.volume = kwargs.get("volume")
        self.complete = kwargs.get("complete")

    def __repr__(self):
         return "CandleLoader()"

    def __str__(self):
        return f"CandleLoader(time:{self.time},c:{self.ask},h:{self.mid},l:{self.bid})"

    @staticmethod
    def from_dict(data):
        data = data.copy()
        if data.get('bid.c') is not None:
            data['bid'] = CandlestickData.from_dict({'o':data['bid.o'],'c':data['bid.c'],'h':data['bid.h'],'l':data['bid.l']})

        if data.get('ask.c') is not None:
            data['ask'] = CandlestickData.from_dict({'o':data['ask.o'],'c':data['ask.c'],'h':data['ask.h'],'l':data['ask.l']})

        if data.get('mid.c') is not None:
            data['mid'] = CandlestickData.from_dict({'o':data['mid.o'],'c':data['mid.c'],'h':data['mid.h'],'l':data['mid.l']})

        return Candle(**data)

class CandlestickData:
    def __init__(self, **kwargs):
        self.o = round(kwargs.get("o"),4)
        self.h = round(kwargs.get("h"),4)
        self.l = round(kwargs.get("l"),4)
        self.c = round(kwargs.get("c"),4)

    def __repr__(self):
         return "CandlestickData()"

    def __str__(self):
        return f"CandlestickData(o:{self.o},c:{self.c},h:{self.h},l:{self.l})"

    @staticmethod
    def from_dict(data):
        data = data.copy()
        return CandlestickData(**data)

class OandaCandleLoader(DataLoader):
    def __init__(self, symbol, account_id, period, time_delta, ctx, N=10):
        self.ctx, self.account_id, self.symbol, self.period, self.time_delta, self.N = ctx, account_id, symbol,period, time_delta, N
    
    def loadData(self):
        while True:
            time = datetime.utcnow()-self.time_delta
            candle_args = {'granularity': self.period, 'price':'MBA',  
                'fromTime' : time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 
                'toTime': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
            # print(f'generator in OandaCandleLoader by time {time}, now: {datetime.utcnow()}, delta:{self.time_delta}')
            candle_resp = self.ctx.instrument.candles(self.symbol, **candle_args)
            if len(candle_resp.get('candles')) == 0:
                yield None
            else:
                for candle in candle_resp.get('candles'):
                    yield candle

class OandaTickLoader(DataLoader):
    def __init__(self, account_id, symbol, ctx):
        self.ctx, self.account_id, self.symbol = ctx, account_id, symbol
    
    def loadData(self):
        price_resp = self.ctx.pricing.stream(self.account_id, snapshot=True, instruments=self.symbol)
        for msg_type, msg in price_resp.parts():
            if msg_type == 'pricing.ClientPrice':
                yield msg 

class CsvCandleLoader(DataLoader):
    __file_prefix__ = "2013-2018_"
    def __init__(self, candle_type, path, converter = None, start=datetime(2013,1,1,0,0,0), end=date.today()):
        super().__init__(candle_type, start, end)
        self.path = Path(path)
        self.converter = converter if converter is not None else CsvCandleLoader.__defalt_converter__

    def loadData(self):
        df = pd.read_csv(self.path / f'{CsvCandleLoader.__file_prefix__}{self.candle_type}.csv')
        df = df[(df['time']>=self.start.strftime('%Y-%m-%d %H:%M:%S')) & (df['time'] < self.end.strftime('%Y-%m-%d %H:%M:%S'))]
        for _, r in df.iterrows():
            yield self.converter(r)
    
    @staticmethod
    def __defalt_converter__(row):
        return Candle.from_dict(row)