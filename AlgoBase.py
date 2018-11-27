
from datetime import datetime, date, timedelta
import pandas as pd
class AlgoBase():

    __supported_time_serires__ = ["S5", "S15", "S30", "M1", "M5", "M15", "M30", "H1", "H2", "H4", "H8", "H12", "D", "W"]
   
    def __init__(self, name, symbol, candles, cash, leverage):
        self.name,self.symbol,self.cash, self.leverage = name, symbol, cash, leverage
        self.candles = self.__sort_time_series__(candles)
        self.accountHandler = AccountHandler(self.cash)
        self.handlers = []

    def __sort_time_series__(self, candles):
       return [ s for s in AlgoBase.__supported_time_serires__ if s in candles]

    def get_candles(self):
        return self.candles
    
    def get_handlers(self):
        return self.handlers
    
    def set_handlers(self, handlers):
        self.handlers = handlers

    def verify(self):
        if len(self.candles) != len(self.handlers): raise ValueError("please assign handlers for all candles before test algorithm")

    def buy(self, time, price, quantity):
        self.accountHandler.buy(time,self.symbol, quantity, price)

    def sell(self, time, price, quantity):
        self.accountHandler.sell(time,self.symbol,quantity,price)

    def close(self, time, price):
        self.accountHandler.close(time, self.symbol, price)

    def get_nav(self):
        return self.accountHandler.get_nav()

    def get_position(self):
        return self.accountHandler.get_position()
    
    def onAlgoEnd(self):
        pass

    def get_account_handler(self):
        return self.accountHandler

    def parameters(self):
        return None

class AccountHandler:

    def __init__(self,cash):
        self.records = pd.DataFrame(columns=["Time", "Symbol","Price","Quantity", "Value","NAV"])
        self.records['Time'] = pd.to_datetime(self.records["Time"])
        self.records['Price'] = pd.to_numeric(self.records['Price'])
        self.records['Quantity'] = pd.to_numeric(self.records['Quantity'])
        self.records['Value'] = pd.to_numeric(self.records['Value'])
        self.records['NAV'] = pd.to_numeric(self.records['NAV'])
        self.records.set_index("Time")
        self.nav = cash
        self.open_position = pd.DataFrame(columns=["Symbol","Price","Quantity", "Value"])
        self.open_position['Price'] = pd.to_numeric(self.open_position['Price'])
        self.open_position['Quantity'] = pd.to_numeric(self.open_position['Quantity'])
        self.open_position['Value'] = pd.to_numeric(self.open_position['Value'])
    
    def __trade__(self, time, symbol, quantity, price, isclose=False):
        if not isclose: print(f"trade {symbol} quantity:{quantity} price:{price} at {time}")
        self.records = self.records.append(pd.DataFrame([[time, symbol, price, quantity, price * quantity, self.nav]], 
            columns=["Time", "Symbol","Price","Quantity", "Value","NAV"]))
        self.open_position = self.open_position.append(pd.DataFrame([[symbol, price, quantity, price*quantity]],columns=["Symbol","Price","Quantity", "Value"]))

    def buy(self, time,symbol, quantity, price):
        if quantity > 0: self.__trade__(time,symbol, quantity, price)

    def sell(self, time, symbol, quantity, price):
        if quantity > 0: self.__trade__(time, symbol, -quantity, price)

    def close(self,time, symbol, price):
        quantity = -self.open_position['Quantity'].sum()
        if quantity != 0:
            self.open_position = self.open_position.append(pd.DataFrame([[symbol, price, quantity, price*quantity ]],columns=["Symbol","Price","Quantity", "Value"]))
            self.nav = self.nav - ( self.open_position["Value"].sum() / price)
            print(f'Close postion {quantity} by {price} at {time}, NAV: {self.nav}')
            self.__trade__(time, symbol,  quantity, price, isclose=True)
            self.open_position = self.open_position.iloc[0:0]

    def get_nav(self):
        return self.nav
    
    def get_position(self):
        return self.open_position["Quantity"].sum()
    
    def save_record(self, name):
        self.records.to_csv(name,float_format='%.5f', columns=["Time", "Symbol","Price","Quantity", "Value","NAV"], index=False)