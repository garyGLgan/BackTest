import pandas as pd
from datetime import date, datetime,timedelta
from pathlib import Path

def get_csv_candle_loader(candle_type, path, start=datetime(2013,1,1,0,0,0), end=date.today()):
    return CsvCandleLoader(candle_type, path, start=start, end=end)

class CandleLoader:
    def __init__(self, candle_type, start=datetime(2013,1,1,0,0,0), end=date.today()):
         self.candle_type = candle_type
         self.start = start
         self.end = end
    
    def loadData(self):
        return []  

class CsvCandleLoader(CandleLoader):
    __file_prefix__ = "2013-2018_"
    def __init__(self, candle_type, path, converter = None, start=datetime(2013,1,1,0,0,0), end=date.today()):
        super().__init__(candle_type, start, end)
        self.path = Path(path)
        self.converter = converter if converter is not None else CsvCandleLoader.__defalt_converter__

    def loadData(self):
        df = pd.read_csv(self.path / f'{CsvCandleLoader.__file_prefix__}{self.candle_type}.csv')
        df['Time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%S')
        df.set_index('Time')
        df = df[(df['Time']>=self.start) & (df['Time'] < self.end)]
        for _, r in df.iterrows():
            yield self.converter(r)
    
    @staticmethod
    def __defalt_converter__(row):
        return {"Ask":{"O":row["ask.o"],"H":row["ask.h"],"L":row["ask.l"],"C":row["ask.c"]},
                "Bid":{"O":row["bid.o"],"H":row["bid.h"],"L":row["bid.l"],"C":row["bid.c"]},
                "Mid":{"O":row["mid.o"],"H":row["mid.h"],"L":row["mid.l"],"C":row["mid.c"]},
                "Time":row["Time"]}