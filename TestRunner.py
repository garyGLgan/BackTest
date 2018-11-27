from datetime import date, datetime, timedelta
from .CandleLoader import get_csv_candle_loader, CandleLoader

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

class TestRunner:
    def __init__(self, algo, start, end):
        self.algo, self.start, self.end= algo, start, end
        self.time_deltas = [ TIME_DELTA_TABLE[c] for c in algo.get_candles()]
        self.candle_handlers = [ algo.get_handlers()[c] for c in algo.get_candles()]
        self.candle_loaders = []

    def local_candle(self, path):
        self.candle_loaders = [ get_csv_candle_loader(c,path, self.start, self.end).loadData() for c in self.algo.get_candles()] 
        self.generators = [self.candle_handle_generator(c,h) for c, h in zip(self.candle_loaders, self.candle_handlers)]

    def candle_handle_generator(self, candle_loader, candle_handler):
        for c in candle_loader:
            yield c["Time"]
            candle_handler(c)
        while  True: yield self.end
    
    def time_step_generator(self):
        time,delta = self.start, self.time_deltas[0]
        while time < (self.end + delta + delta):
            yield time
            time = time + delta

    def report_name(self):
        return f"{self.algo.name.replace(' ', '_')}-{str(self.algo.parameters()).replace(')','').replace('(','').replace(',','_')}-{self.start.strftime('%Y-%m-%d')}-{self.end.strftime('%Y-%m-%d')}.csv"

    def run(self, save_report=True):
        self.algo.verify()
        time_step = self.time_step_generator()
        last_times = [ next(t) for t in self.generators]
        for time in time_step:
            last_times = [ l_time if l_time + delta > time else next(candle) 
                for candle, delta, l_time in zip(self.generators, self.time_deltas, last_times)]

        self.algo.onAlgoEnd()
        if save_report: self.algo.get_account_handler().save_record(self.report_name())
        print("back test finished")