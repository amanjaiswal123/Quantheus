from source.Backtest_v1 import Backtest
from source.Realtime_v1 import Realtime
import pandas
from source.Commons import notify, TradingDays
import pandas
from datetime import datetime, timedelta
from source.Commons import _datadate,TradingDays,datadateslice
import matplotlib.pyplot as plt
from source.Indicators import  _TrendID,_PeaksTroughs
def UpTrend(Fields, Portfolio, datadate, firstcalc, YSignals=None):
    global EOV
    TodaysClose = Fields['close'][(Fields['close'].index.get_level_values('dat') == datadate)].reset_index('dat')['close']
    TodaysAdjustedClose = Fields['adj_close'][(Fields['adj_close'].index.get_level_values('dat') == datadate)].reset_index('dat')['adj_close']
    Volume90 = Fields['volume'][(Fields['volume'].index.get_level_values('dat') >= TradingDays[TradingDays.index(datadate)-60])].groupby(['ticker','exchange']).mean()
    TodaysVolume = Fields['volume'][Fields['volume'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns=['dat'])['volume']
    Today80 = datadateslice(Fields['adj_close'],End_Date=datadate,Trading_Days=80)
    Trends = _TrendID(Today80, datadate, 4, 'up', .005)
    DatetimeDataDate = datetime.strptime(datadate, '%Y-%m-%d')
    Duration = (pandas.to_datetime(Trends) - DatetimeDataDate).dt.days.abs()
    TrendingData = Today80.reset_index('dat').copy()
    TrendingData['Date'] = Trends
    TrendingData = TrendingData[(TrendingData['dat'] >= TrendingData['Date'])].reset_index().set_index(['ticker','exchange','dat']).drop(columns=['Date'])
    OverallGain = TrendingData.groupby(['ticker', 'exchange']).head(1).reset_index('dat')['adj_close']
    OverallGain = (TrendingData.groupby(['ticker', 'exchange']).tail(1).reset_index('dat')['adj_close']-OverallGain)/OverallGain
    TrendingData.reset_index('dat',inplace=True)
    LinerGrowthRate = OverallGain / TrendingData.groupby(['ticker', 'exchange']).size()
    TrendingData['Linear Growth Rate'] = LinerGrowthRate
    TrendingData = TrendingData.reset_index().set_index(['ticker','exchange','dat'])
    TrendingData['Position'] = TrendingData['adj_close'].groupby(['ticker','exchange']).cumcount()+1
    TrendingData['GrowthRxPosition'] = (TrendingData['Linear Growth Rate']*TrendingData['Position'])
    TrendingData = TrendingData.sort_index(level=['ticker','exchange','dat']).reset_index('dat')
    TrendingData['First Rally Price'] = TrendingData['adj_close'].groupby(['ticker','exchange']).head(1)
    TrendingData = TrendingData.reset_index().set_index(['ticker','exchange','dat'])
    TrendingData['Linear Growth Price'] = (TrendingData['GrowthRxPosition']*TrendingData['First Rally Price'])+TrendingData['First Rally Price']
    TrendingData['Linear Growth Price'][TrendingData['Position'] == 1] = TrendingData['First Rally Price']
    TrendingData['Deviation'] = ((TrendingData['Linear Growth Price']-TrendingData['adj_close'])/TrendingData['Linear Growth Price']).abs()
    AverageDeviationSize = TrendingData['Deviation'].groupby(['ticker','exchange']).size()-2
    AverageDeviationSize[AverageDeviationSize <= 0] = 0
    AverageDeviation = TrendingData['Deviation'].groupby(['ticker','exchange']).sum()/(AverageDeviationSize)
    TrendingData.drop(columns=['Linear Growth Price','Position','Linear Growth Rate','GrowthRxPosition'],inplace=True)

    dataday2 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=2)
    DailyPCTChange = dataday2.sort_index(level=['ticker','exchange','dat']).groupby(['ticker','exchange']).pct_change().dropna().reset_index('dat').drop(columns=['dat'])['adj_close']

    Data365 = datadateslice(Fields['adj_close'],End_Date=datadate,Days=365)
    PeaksTroughs = _PeaksTroughs(Data365)
    PotentialGain = (PeaksTroughs['Peak']*.9-TodaysAdjustedClose)/TodaysAdjustedClose
    Pipe = pandas.DataFrame(data={'Rally Linear Growth Rate':LinerGrowthRate,'Rally Average Deviation':AverageDeviation, 'Duration':Duration,"Today's Close":TodaysClose,"Today's Adjusted Close":TodaysAdjustedClose,'Daily PCT Change':DailyPCTChange,'90 Day Volume Average':Volume90,"Today's Volume":TodaysVolume,'Price Position':PricePosition,'Potential Gain':PotentialGain,'Action':'Nothing','Allocation':.01,'Stop Limit Percent':-1,'Holding Period':60})
    InPortfolio = Pipe.index.isin(Portfolio.index)
    #Gain = ((Pipe["Today's Close"][InPortfolio] - Portfolio['Buy Price'])/Portfolio['Buy Price']) >= .01
    BuyFilter = (Pipe['Price Position'] >= .25) & (Pipe['Price Position'] <= .5) & (Pipe['Rally Linear Growth Rate'] >= .005) & (Pipe['Rally Average Deviation'] <= .03) & (Pipe['Duration'] >= 15) & (Pipe['90 Day Volume Average'] >= 50000)
    SellFilter = Pipe['Price Position'] >= .9
    Pipe['Action'][BuyFilter] = 'Buy'
    Pipe['Action'][SellFilter] = 'Sell'
    Pipe.sort_values('Potential Gain',ascending=False,inplace=True)
    return Pipe
Backtest(UpTrend,Days=90,StartingOffset=365,SC=30000,BacktestTitle='RidingUpTrends30k365Offest90')
