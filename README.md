# BackTest
QuantBackTest

A back test framework for quant trading, tested based on USDJPY'candle
TBD: load data from a rest api, draw figure.

1. Create algorithm

```python
from BackTest.AlgoBase import AlgoBase

class CrossHighLow(AlgoBase):
    def __init__(self, symbol, cash, leverage):
        super().__init__("Cross High Low",symbol, ["M30", "M1"], cash, leverage)
        self.set_handlers({"M30": self.onM30Data, "M1":self.onM1Data})
        
    def onM1Data(self, candle):
        # action when receive M1 candle
        pass
    
    def onM30Data(self, candle):
        # action when receive M30 candle
        pass
 ```
 
 2. Init test runner
 
 ```python
 algo = CrossHighLow("USDJPY",10000,50) # create instance of algorithm
 runner = TestRunner(algo, datetime(2013,1,1,0,0,0), datetime(2013,8,1,0,0,0)) # instance of runner
 runer.local_candle("data") # load candle data from data folder
 ```
 
 3. Run back test
 
 ```python
runner.run()
```

4. Get the report at current folder

