import pandas as pd
from telegram_send import send

def get_account_handler(typ='test', cash=10000, leverage = 50, account_id = None, ctx = None):
    if typ=='test':
        return LocalAccountHandler(cash,leverage)
    elif typ== 'oanda':
        if ctx is None:
            raise RuntimeError("client cann't be null for a remote account")
        else:
            return OandaAccountHandler(account_id, ctx)
    else:
        raise RuntimeError(f"unsopported account type {typ}")

class AccountHandler:
    def __init__(self):
        self.__nav__ = 0
        self.__leverage__ = 0
        self.records = pd.DataFrame(columns=["Time", "Symbol","Price","Quantity", "Value","NAV"])
        self.records['Time'] = pd.to_datetime(self.records["Time"])
        self.records['Price'] = pd.to_numeric(self.records['Price'])
        self.records['Quantity'] = pd.to_numeric(self.records['Quantity'])
        self.records['Value'] = pd.to_numeric(self.records['Value'])
        self.records['NAV'] = pd.to_numeric(self.records['NAV'])
        self.records.set_index("Time")
        self.open_position = pd.DataFrame(columns=["Symbol","Price","Quantity", "Value"])
        self.open_position['Price'] = pd.to_numeric(self.open_position['Price'])
        self.open_position['Quantity'] = pd.to_numeric(self.open_position['Quantity'])
        self.open_position['Value'] = pd.to_numeric(self.open_position['Value'])
    
    def save_record(self,time, symbol, price, quantity):
        self.records = self.records.append(pd.DataFrame([[time, symbol, price, quantity, price * quantity, self.__nav__]], 
            columns=["Time", "Symbol","Price","Quantity", "Value","NAV"]))
        self.open_position = self.open_position.append(pd.DataFrame([[symbol, price, quantity, price*quantity]],columns=["Symbol","Price","Quantity", "Value"]))

    def reset_open_postion(self):
        self.open_position = self.open_position.iloc[0:0]

    def get_nav(self):
        return round(self.__nav__,4)
    
    def get_leverage(self):
        return self.__leverage__

    def write_record(self, name):
        self.records.to_csv(name,float_format='%.5f', columns=["Time", "Symbol","Price","Quantity", "Value","NAV"], index=False)

    def get_position(self,symbol):
        return self.open_position["Quantity"].sum()

class OandaAccountHandler(AccountHandler):
    def __init__(self, account_id, ctx):
        super(OandaAccountHandler,self).__init__()
        self.ctx, self.account_id = ctx, account_id
        account = self.get_account_summary(ctx)
        self.__nav__= account.marginCloseoutNAV
        self.__leverage__ = account.marginRate
    
    def __trade__(self,time, symbol, quantity):
        order = {"instrument": symbol, "units":quantity}
        resp = self.ctx.order.market(self.account_id,**order)
        if resp.status == 200 and hasattr(resp, "orderFillTransaction"):
            send([f'{self.account_id} submited trade ID: {str(resp.get("lastTransactionID"))}'] )
            self.save_record(time,symbol, resp.get("orderFillTransaction").price, quantity)

    def refresh_nav(self):
        acco = self.get_account_summary(self.ctx)
        self.__nav__= acco.balance

    def buy(self, time,symbol, quantity, price=None):
        self.__trade__(time,symbol,quantity)
        
    def sell(self, time, symbol, quantity, price=None):
        self.__trade__(time,symbol,-quantity)

    def close(self,time, symbol, price=None):
        quantity = -self.open_position['Quantity'].sum()
        if quantity != 0:
            order = {"instrument": symbol, "units": -quantity}
            resp = self.ctx.order.market(self.account_id,**order)
            if resp.status == 200 and hasattr(resp, "orderFillTransaction"):
                send([f'{self.account_id} submited trade ID: {str(resp.get("lastTransactionID"))}'] )
                self.save_record(time,symbol, resp.get("orderFillTransaction").price, quantity)
                self.refresh_nav()
                self.reset_open_postion()

    def get_account_summary(self, ctx):
        resp = ctx.account.summary(self.account_id)
        return resp.get('account')
        
class LocalAccountHandler(AccountHandler):
    def __init__(self,cash, leverage):
        super(LocalAccountHandler, self).__init__()
        self.__leverage__ = leverage
        self.__nav__ = cash
        
    def __trade__(self, time, symbol, quantity, price, isclose=False):
        if not isclose: print(f"trade {symbol} quantity:{quantity} price:{price} at {time}")
        self.save_record(time,symbol, price, quantity)
    
    def buy(self, time,symbol, quantity, price):
        if quantity > 0: self.__trade__(time,symbol, quantity, price)

    def sell(self, time, symbol, quantity, price):
        if quantity > 0: self.__trade__(time, symbol, -quantity, price)

    def close(self,time, symbol, price):
        quantity = -self.open_position['Quantity'].sum()
        if quantity != 0:
            self.save_record(time,symbol,price, quantity)
            self.__nav__ = self.__nav__ - ( self.open_position["Value"].sum() / price)
            print(f'Close postion {quantity} by {price} at {time}, NAV: {self.get_nav()}')


    
    
