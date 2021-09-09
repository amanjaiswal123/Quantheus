from source.Backtest import Backtest
#from source.Realtime import Realtime
import pandas
import numpy


def algo(Fields, Portfolio, aval_cash, net_worth, datadate, first_calc, YSignals=None):
    TodaysClose = Fields['close'][(Fields['close'].index.get_level_values('dat') == datadate)].reset_index('dat')['close']
    TodaysAdjustedClose = Fields['adj_close'][(Fields['adj_close'].index.get_level_values('dat') == datadate)].reset_index('dat')['adj_close']
    Pipe = pandas.DataFrame(data={"Today's Close":TodaysClose,"Today's Adjusted Close":TodaysAdjustedClose,'Quantity':0})
    Pipe["Quantity"] = numpy.random.randint(-5,20,Pipe.shape[0])

    Pipe["Stop Limit"] = Pipe["Today's Adjusted Close"]*1.1
    Pipe["Stop Limit"][Pipe["Quantity"] < 0] = Pipe["Today's Adjusted Close"]*.9

    Pipe["Stop Loss"] = Pipe["Today's Adjusted Close"]*.9
    Pipe["Stop Loss"][Pipe["Quantity"] < 0] = Pipe["Today's Adjusted Close"]*1.1
    Pipe = Pipe.sample(frac=1)
    return Pipe.head(100)
#Realtime(UpTrend,365*5)
Backtest(algo,End_Date='2012-01-01',Start_Date='2007-06-01',StartingOffset=365*5,SC=200000,marg_call_protect=True,BacktestTitle='PeakTroughP60MKSTPLMT')
