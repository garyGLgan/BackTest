import pandas as pd
from telegram_send import send

def get_account_handler(typ='test', cash=10000, leverage = 50, account_id = None, ctx = None, record_path=None):
    if typ=='test':
        return LocalAccountHandler(cash,leverage, record_path)
    elif typ== 'oanda':
        if ctx is None:
            raise RuntimeError("client cann't be null for a remote account")
        else:
            return OandaAccountHandler(account_id, ctx, record_path)
    else:
        raise RuntimeError(f"unsopported account type {typ}")

class AccountHandler:
    def __init__(self, record_path):
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
        self.write_record, self.record_path = False, None
        self.set_record_path(record_path)
        
    def set_record_path(self, record_path):
        if record_path is None:
            self.write_record, self.record_path = False, None
        else:
            self.record_path = record_path
            self.records.to_csv(record_path)
            self.write_record = True

    def __write_row__(self, df):
        with open(self.record_path, 'a') as f:
            df.to_csv(f, header=False)
            f.close()

    def __save_position__(self,time, symbol, price, quantity):
        row = pd.DataFrame([[symbol, price, quantity, price*quantity]],columns=["Symbol","Price","Quantity", "Value"])
        self.open_position = self.open_position.append(row)

    def __save_record__(self,time, symbol, price, quantity):
        record_row = pd.DataFrame([[time, symbol, price, quantity, price * quantity, self.__nav__]], columns=["Time", "Symbol","Price","Quantity", "Value","NAV"])
        self.records = self.records.append(record_row)
        if self.write_record: self.__write_row__(record_row)
        
    def reset_open_postion(self):
        self.open_position = self.open_position.iloc[0:0]

    def get_nav(self):
        return round(self.__nav__,4)
    
    def get_leverage(self):
        return self.__leverage__

    def get_position(self,symbol):
        return self.open_position["Quantity"].sum()

class OandaAccountHandler(AccountHandler):
    def __init__(self, account_id, ctx, record_path):
        super(OandaAccountHandler,self).__init__(record_path)
        self.ctx, self.account_id = ctx, account_id
        self.refresh_nav()
        print(f'account({self.account_id}) nav:{self.__nav__} leverage:{self.__leverage__}')
    
    def __trade__(self,time, symbol, quantity):
        order = {"instrument": symbol, "units":quantity}
        resp = self.ctx.order.market(self.account_id,**order)
        if resp.status == 200 :
            price = resp.get("orderFillTransaction").price
            print(f'submitted trade quantity:{quantity} price:{price}')
            send([f"account({self.account_id}) trade {symbol} quantity:{quantity} price:{price} at {time}"])
            self.__save_position__(time,symbol, price, quantity)
            self.__save_record__(time,symbol, price, quantity)

    def refresh_nav(self):
        account = self.get_account_summary(self.ctx)
        self.__nav__= account.balance
        self.__leverage__ = int(1 / account.marginRate)

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
                price = resp.get("orderFillTransaction").price
                self.__save_position__(time,symbol, price, quantity)
                self.refresh_nav()
                send([f'"account({self.account_id}) Close postion {quantity} by {price} at {time}, NAV: {self.get_nav()}'] )
                self.__save_record__(time,symbol, price, quantity)
                self.reset_open_postion()

    def get_account_summary(self, ctx):
        resp = ctx.account.summary(self.account_id)
        return resp.get('account')
        
class LocalAccountHandler(AccountHandler):
    def __init__(self,cash, leverage, record_path):
        super(LocalAccountHandler, self).__init__(record_path)
        self.__leverage__ = leverage
        self.__nav__ = cash
        
    def __trade__(self, time, symbol, quantity, price, isclose=False):
        print(f"trade {symbol} quantity:{quantity} price:{price} at {time}")
        self.__save_position__(time,symbol, price, quantity)
        self.refresh_nav(price)
        self.__save_record__(time,symbol, price, quantity)

    def buy(self, time,symbol, quantity, price):
        if quantity > 0 and self.__nav__ > 0: self.__trade__(time,symbol, quantity, price)

    def sell(self, time, symbol, quantity, price):
        if quantity > 0 and self.__nav__ > 0: self.__trade__(time, symbol, -quantity, price)

    def close(self,time, symbol, price):
        quantity = -self.open_position['Quantity'].sum()
        if quantity != 0:
            self.__save_position__(time,symbol, price, quantity)
            self.refresh_nav(price)
            self.__save_record__(time,symbol, price, quantity)
            self.reset_open_postion()
            print(f'Close postion {quantity} by {price} at {time}, NAV: {self.get_nav()}')
    
    def refresh_nav(self, price):
        self.__nav__ = self.__nav__ - ( self.open_position["Value"].sum() / price)


    
    
