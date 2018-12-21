import pandas as pd
from datetime import date, datetime,timedelta
from pathlib import Path
import pdb 
import pymongo
class Candle:
    def __init__(self, time, ask, mid, bid, volume):
        self.time, self.ask, self.mid, self.bid, self.volume = time, ask, mid, bid, volume 
        
    def __repr__(self):
         return "CandleLoader()"

    def __str__(self):
        return f"CandleLoader(time:{self.time},c:{self.ask},h:{self.mid},l:{self.bid})"

    @staticmethod
    def from_dict(data):
        bid = CandlestickData.from_dict({'o':data['bid.o'],'c':data['bid.c'],'h':data['bid.h'],'l':data['bid.l']})
        ask = CandlestickData.from_dict({'o':data['ask.o'],'c':data['ask.c'],'h':data['ask.h'],'l':data['ask.l']})
        mid = CandlestickData.from_dict({'o':data['mid.o'],'c':data['mid.c'],'h':data['mid.h'],'l':data['mid.l']})
        return Candle(data['time'],ask, mid, bid, data['volume'])

    @staticmethod
    def from_json(json):
        data = json 
        ask = CandlestickData.from_dict(data['ask'])
        mid = CandlestickData.from_dict(data['mid'])
        bid = CandlestickData.from_dict(data['bid'])
        return Candle(data['time'], ask, mid, bid, data['volume'])

class CandlestickData:
    def __init__(self, o, c, h, l):
        self.o, self.c, self.h, self.l = o, c, h, l

    def __repr__(self):
         return "CandlestickData()"

    def __str__(self):
        return f"CandlestickData(o:{self.o},c:{self.c},h:{self.h},l:{self.l})"

    @staticmethod
    def from_dict(data):
        return CandlestickData(data['o'], data['c'], data['h'],data['l'])

class DataLoader:
    def __init__(self, candle_type, start=datetime(2013,1,1,0,0,0), end=date.today()):
         self.candle_type = candle_type
         self.start = start
         self.end = end
    
    def loadData(self):
        return []
    

class OandaHistoryLoader(DataLoader):
    def __init__(self, symbol,  period, ctx, start=datetime(2013,1,1,0,0,0), end=date.today(), converter=None):
        self.ctx,  self.symbol, self.period, self.start, self.end, self.converter  = ctx,  symbol, period, start, end, converter
        
    def loadData(self):
        time_step = timedelta(hours = 4) if self.period.startswith('S') else timedelta(days=1)
        kwargs = {}

        kwargs['granularity'] = self.period
        kwargs['price'] = 'MBA'
        time = self.start
        retry = True 

        while time < self.end:
            kwargs['fromTime'] = time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            kwargs['toTime'] = (time + time_step).strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
            retry = True
            while retry:
                try: 
                    response = self.ctx.instrument.candles(self.symbol, **kwargs)
                    if response.status ==200:
                        retry = False
                        time = time + time_step
                        if len(response.get("candles"))>0: 
                            for candle in response.get("candles"):
                               # pdb.set_trace()
                                yield candle if self.converter is None else self.converter(candle) 
                except Exception as e: 
                    print(e)
                    print(f"{kwargs['fromTime']}, {self.period}, failed retry")
        
class OandaCandleLoader(DataLoader):
    def __init__(self, symbol, account_id, period, time_delta, ctx ):
        self.ctx, self.account_id, self.symbol, self.period, self.time_delta  = ctx, account_id, symbol,period, time_delta 
    
    def loadData(self):
        while True:
            time = datetime.utcnow()-self.time_delta
            candle_args = {'granularity': self.period, 'price':'MBA',  
                'fromTime' : time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 
                'toTime': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
            # print(f'generator in OandaCandleLoader by time {time}, now: {datetime.utcnow()}, delta:{self.time_delta}')
            try:
                candle_resp = self.ctx.instrument.candles(self.symbol, **candle_args)
                if len(candle_resp.get('candles')) == 0:
                    yield None
                else:
                    for candle in candle_resp.get('candles'):
                        candle.time = candle.time[:candle.time.find('.')]
                        yield candle
            except Exception as e:
                print(f'exception {e}')
                yield None

class OandaTickLoader(DataLoader):
    def __init__(self, account_id, symbol, ctx):
        self.ctx, self.account_id, self.symbol = ctx, account_id, symbol
    
    def loadData(self):
        while True:
            try:
                price_resp = self.ctx.pricing.stream(self.account_id, snapshot=True, instruments=self.symbol)
                for msg_type, msg in price_resp.parts():
                    if msg_type == 'pricing.ClientPrice':
                        msg.time = msg.time[:msg.time.find('.')]
                        yield msg
            except Exception as e:
                print(f"exception : {e}")

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

class OandaMongoCandleLoader(DataLoader):
    def __init__(self, symbol, period, mongodb, start=datetime(2013,1,1), end=date.today(), converter= None):
        super().__init__(period, start, end)
        self.db, self.symbol, self.converter = mongodb, symbol, converter
        self.collection = self.db.get_collection(f'{self.symbol}_{self.candle_type}')
   
    def loadData(self):
        _from = self.start
        _to = datetime(_from.year+1, 1,1)
        if _to > self.end: _to = self.end
        while True: 
            filter = {'$and':[{'time':{'$gte':_from.strftime('%Y-%m-%dT%H:%M:%S')}},{'time':{'$lt':_to.strftime('%Y-%m-%dT%H:%M:%S')}}]}
            for c in self.collection.find(filter).sort([( 'time',pymongo.ASCENDING)]) :
                yield c if self.converter is None else self.converter(c)
            if _to >=self.end: break
            _from = _to
            _to = datetime(_from.year+1, 1,1)
            if _to > self.end: _to = self.end



def get_csv_candle_loader(symbol, candle_type, path, start=datetime(2013,1,1,0,0,0), end=date.today(),converter = None):
    return CsvCandleLoader(candle_type, path, start=start, end=end, converter=converter)

def get_oanda_candle_loader(symbol, account_id, period, time_delta, ctx):
    return OandaCandleLoader(symbol, account_id, period, time_delta, ctx)

def get_oanda_tick_loader(account_id, symbol, ctx):
    return OandaTickLoader(account_id, symbol, ctx)

def get_oanda_history_loader(symbol,  period, ctx, start=datetime(2013,1,1,0,0,0), end=date.today(), converter = None):
    return OandaHistoryLoader(symbol, period, ctx, start, end, converter)

def get_mongo_oanda_loader(symbol, period, db, start=datetime(2013,1,1,0,0,0), end=date.today(), converter = Candle.from_json):
    return OandaMongoCandleLoader(symbol, period, db, start, end, converter)




