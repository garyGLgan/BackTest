from datetime import date, datetime, timedelta
import pdb
from .DataLoader import *
from bson.json_util import loads

TIME_DELTA_TABLE = {"S5": timedelta(seconds = 5), 
                        "S15": timedelta(seconds = 15),
                        "S30": timedelta(seconds = 30),
                        "M1": timedelta(minutes=1),
                        "M5": timedelta(minutes=5),
                        "M15": timedelta(minutes=15),
                        "M30": timedelta(minutes=30),
                        "H1": timedelta(hours=1),
                        "H2": timedelta(hours=2),
                        "H4": timedelta(hours=4),
                        "H8": timedelta(hours=8),
                        "H12": timedelta(hours=12),
                        "D": timedelta(days=1),
                        "W": timedelta(days=7)}

class TestTick:
    def __init__(self, time, ask, bid):
        self.time = time 
        self.closeoutBid = round(bid,4)
        self.closeoutAsk = round(ask,4)
 
    def __repr__(self):
         return "CandleLoader()"

    def __str__(self):
        return f"CandleLoader(time:{self.time},bid:{self.closeoutBid},ask:{self.closeoutAsk})"

    @staticmethod
    def from_dict(data):
        data = data.copy()
        return TestTick(data.get("time"), data.get("ask.c"), data.get("bid.c"))
    
    @staticmethod
    def from_candlestick(stick):
        return TestTick(stick.time,stick.ask.c, stick.bid.c)

    @staticmethod
    def from_json(json):
        return TestTick(json['time'], json['ask']['c'], json['bid']['c'])

class AlgoRunner:
    def __init__(self, algo):
        self.algo = algo
        self.time_deltas = [ TIME_DELTA_TABLE[c] for c in algo.get_candles()]
        self.candle_handlers = [ algo.get_handlers()[c] for c in algo.get_candles()]

    def run(self, save_report=True): pass

    @staticmethod 
    def get_test_runner(algo,start,end, path=None, ctx=None, mongodb = None, tick_candle="M1", tick_conveter= TestTick.from_dict, data_source='local'):
        return TestRunner(algo, start,end, path, mongodb, tick_candle, tick_conveter, ctx, data_source)

    @staticmethod
    def get_oanda_runner(algo,account_id, ctx, ctx_stream):
        return OandaRunner(algo, account_id, ctx, ctx_stream)

    def parseDatetime(self, msg):
        #return datetime.strptime(msg[:-4]+msg[-1],'%Y-%m-%dT%H:%M:%S.%fZ')
        return datetime.strptime(msg ,'%Y-%m-%dT%H:%M:%S')

class OandaRunner(AlgoRunner):
    def __init__(self, algo, account_id, ctx, ctx_stream):
        super(OandaRunner, self).__init__(algo)
        self.tick_generator = get_oanda_tick_loader(account_id, self.algo.symbol, ctx_stream).loadData()
        self.candle_loaders = [ get_oanda_candle_loader(self.algo.symbol, account_id, c, d,ctx ).loadData() for d, c in zip (self.time_deltas, self.algo.get_candles())]
        self.generators = [self.candle_handle_generator(c,h, d) for c, h, d in zip(self.candle_loaders, self.candle_handlers, self.time_deltas)]

    def candle_handle_generator(self, candle_loader, candle_handler, time_delta):
        for c in candle_loader:
            if c is None:
                yield datetime.utcnow()-time_delta
            else:
                candle_handler(c)
                yield self.parseDatetime(c.time) 

    def run(self, save_report= True):
        last_times = [datetime.utcnow() for i in self.algo.get_candles() ]
        for tick in self.tick_generator:
            time = self.parseDatetime(tick.time)
            last_times = [ l_time if l_time + delta + delta > time else next(candle) for candle, delta, l_time in zip(self.generators, self.time_deltas, last_times)]
            self.algo.on_tick(tick)

   
        
class TestRunner(AlgoRunner):
    
    def __init__(self, algo, start, end, path=None,mongodb=None, tick_candle="M1", tick_conveter = TestTick.from_dict, ctx = None, data_source='local'):
        super(TestRunner,self).__init__(algo)
        self.start, self.end = start, end
        self.time_deltas =[TIME_DELTA_TABLE[tick_candle]] + self.time_deltas
        self.candle_handlers = [algo.on_tick] + self.candle_handlers
        self.candle_loaders = self.__data_loader__(data_source, path, ctx, mongodb, tick_candle, tick_conveter)
        self.generators = [self.candle_handle_generator(c,h) for c, h in zip(self.candle_loaders, self.candle_handlers)]
        
    def candle_handle_generator(self, candle_loader, candle_handler):
        for c in candle_loader:
            yield c.time
            candle_handler(c)
        while  True: yield self.end.strftime('%Y-%m-%dT%H:%M:%S' )
    
    def __data_loader__(self, data_source, path, ctx, db, tick_candle,  tick_conveter):
            if data_source == 'local':
                return [get_csv_candle_loader(self.algo.symbol, tick_candle, path, start=self.start, end=self.end, converter=tick_conveter).loadData()] + [ get_csv_candle_loader(self.algo.symbol, c, path, self.start, self.end).loadData() for c in self.algo.get_candles()]
            elif data_source == 'mongo':
                return [get_mongo_oanda_loader(self.algo.symbol, tick_candle, db, start=self.start, end=self.end, converter=tick_conveter).loadData()] + [ get_mongo_oanda_loader(self.algo.symbol, c, db, start=self.start, end=self.end ).loadData() for c in self.algo.get_candles()]     
            else: 
                return [get_oanda_history_loader(self.algo.symbol, tick_candle, ctx, start=self.start, end=self.end, converter=tick_conveter).loadData()] + [ get_oanda_history_loader(self.algo.symbol, c, ctx, self.start, self.end).loadData() for c in self.algo.get_candles()] 
    def time_step_generator(self):
        time,delta = self.start, self.time_deltas[0]
        while time < (self.end + delta + delta):
            yield time
            time = time + delta

    # def report_name(self):
    #     return f"{self.algo.name.replace(' ', '_')}-{str(self.algo.get_param_str()}-{self.start.strftime('%Y-%m-%d')}-{self.end.strftime('%Y-%m-%d')}.csv"

    def run(self, save_report=True):
        self.algo.verify()
        time_step = self.time_step_generator()
        last_times = [ self.parseDatetime(next(t)) for t in self.generators]
        for time in time_step:
            last_times = [ l_time if l_time + delta > time else self.parseDatetime(next(candle)) 
                for candle, delta, l_time in zip(self.generators, self.time_deltas, last_times)]

        self.algo.on_algo_end()
        print("back test finished")