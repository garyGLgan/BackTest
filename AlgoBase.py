from datetime import datetime, date, timedelta
import pandas as pd
from .AccountHandler import AccountHandler
class AlgoBase():

    __supported_time_serires__ = ["S5", "S15", "S30", "M1", "M5", "M15", "M30", "H1", "H2", "H4", "H8", "H12", "D", "W"]
   
    def __init__(self, name, symbol, candle_handlers, account_handler):
        self.name,self.symbol,self.account_handler,self.candle_handlers = name, symbol, account_handler, candle_handlers
        self.candles = self.__sort_time_series__(candle_handlers.keys())

    def __sort_time_series__(self, candles):
       return [ s for s in AlgoBase.__supported_time_serires__ if s in candles]

    def get_candles(self):
        return self.candles
    
    def get_handlers(self):
        return self.candle_handlers
    
    # def set_handlers(self, handlers):
    #     self.handlers = handlers
   
    def verify(self):
        if len(self.candles) != len(self.candle_handlers): raise ValueError("please assign handlers for all candles before test algorithm")

    # def buy(self, time, price, quantity):
    #     self.account_handler.buy(time,self.symbol, quantity, price)

    # def sell(self, time, price, quantity):
    #     self.account_handler.sell(time,self.symbol,quantity,price)

    # def close(self, time, price):
    #     self.account_handler.close(time, self.symbol, price)

    # def get_nav(self):
    #     return self.account_handler.get_nav()

    # def get_position(self):
    #     return self.account_handler.get_position()
    
    def on_algo_end(self):
        pass

    def parameters(self):
        return None
     
    def on_tick(self, tick):
        pass

   