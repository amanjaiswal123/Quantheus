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
    TodaysClose = Fields['close'][(Fields['close'].index.get_level_values('dat') == datadate)].reset_index('dat')['close']
    TodaysAdjustedClose = Fields['adj_close'][(Fields['adj_close'].index.get_level_values('dat') == datadate)].reset_index('dat')['adj_close']
    Volume90 = Fields['volume'][(Fields['volume'].index.get_level_values('dat') >= TradingDays[TradingDays.index(datadate)-60])].groupby(['ticker','exchange']).mean()
    TodaysVolume = Fields['volume'][Fields['volume'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns=['dat'])['volume']

    dataday2 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=2)
    DailyPCTChange = dataday2.sort_index(level=['ticker','exchange','dat']).groupby(['ticker','exchange']).pct_change().dropna().reset_index('dat').drop(columns=['dat'])['adj_close']

    Data90 = datadateslice(Fields['adj_close'],End_Date=datadate,Days=365)
    PeaksTroughs = _PeaksTroughs(Data90,PeakGap=10,Range=.01)
    PricePosition = (TodaysAdjustedClose-PeaksTroughs['Trough'])/(PeaksTroughs['Peak']-PeaksTroughs['Trough'])
    PotentialGain = (PeaksTroughs['Peak']*.9-TodaysAdjustedClose)/TodaysAdjustedClose

    Fields = Fields.reset_index()
    Fields['date'] = Fields['dat']
    Fields.set_index(['ticker', 'exchange'], inplace=True)
    Fields['Peak'] = PeaksTroughs['Peak']
    Fields['Trough'] = PeaksTroughs['Trough']
    Fields.reset_index(inplace=True)
    Fields.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    Fields['Price Position'] = (Fields['adj_close'] - Fields['Trough']) / (Fields['Peak'] - Fields['Trough'])

    #Data365 = datadateslice(Fields['adj_close'],End_Date=datadate,Days=365)
    #YearlyChange = Data365.groupby(['ticker','exchange']).head(1).reset_index('dat')['adj_close']
    #YearlyChange = (Data365.groupby(['ticker','exchange']).tail(1).reset_index('dat')['adj_close'] - YearlyChange) / YearlyChange

    Today80 = datadateslice(Fields['adj_close'],End_Date=datadate,Trading_Days=90)
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
    #TrendingData['Linear Growth Rate'] = LinerGrowthRate
    #TrendingData = TrendingData.reset_index().set_index(['ticker','exchange','dat'])
    #TrendingData['Position'] = TrendingData['adj_close'].groupby(['ticker','exchange']).cumcount()+1
    #TrendingData['GrowthRxPosition'] = (TrendingData['Linear Growth Rate']*TrendingData['Position'])
    #TrendingData = TrendingData.sort_index(level=['ticker','exchange','dat']).reset_index('dat')
    #TrendingData['First Rally Price'] = TrendingData['adj_close'].groupby(['ticker','exchange']).head(1)
    #TrendingData = TrendingData.reset_index().set_index(['ticker','exchange','dat'])
    #TrendingData['Linear Growth Price'] = (TrendingData['GrowthRxPosition']*TrendingData['First Rally Price'])+TrendingData['First Rally Price']
    #TrendingData['Linear Growth Price'][TrendingData['Position'] == 1] = TrendingData['First Rally Price']
    #TrendingData['Deviation'] = ((TrendingData['Linear Growth Price']-TrendingData['adj_close'])/TrendingData['Linear Growth Price'])
    #AverageDeviationSize = TrendingData['Deviation'].groupby(['ticker','exchange']).size()-2
    #AverageDeviationSize[AverageDeviationSize <= 0] = 0
    #AverageNegativeDeviation = TrendingData['Deviation'][TrendingData['Deviation'] < 0].abs().groupby(['ticker','exchange']).sum()/(AverageDeviationSize)
    #TrendingData.drop(columns=['Linear Growth Price','Position','Linear Growth Rate','GrowthRxPosition'],inplace=True)

    PeakTrough = Fields[(Fields['Price Position'] <= .1) & (Fields['Price Position'] >= -.69) | (Fields['Price Position'] >= .9) & (Fields['Price Position'] <= 1.69)]
    PeakTrough['diff'] = PeakTrough['Price Position'].groupby(['ticker', 'exchange']).diff().abs()
    temp = PeakTrough.groupby(['ticker', 'exchange']).head(1)
    temp = temp.append(PeakTrough[PeakTrough['diff'] >= .8]).sort_index(level=['ticker', 'exchange', 'dat'])
    temp['Date diff'] = pandas.to_datetime(temp['date']).groupby(['ticker','exchange']).diff().dt.days

    #CycleChange = temp.groupby(['ticker', 'exchange']).tail(1).reset_index('dat')[['adj_close', 'date']]
    #CycleChange['PCT'] = (TodaysAdjustedClose-CycleChange['adj_close'])/CycleChange['adj_close']
    #CycleDuration = ((datetime.strptime(datadate, '%Y-%m-%d') - pandas.to_datetime(CycleChange['date'])).dt.days)
    #CycleDeviation = Fields.reset_index('dat')
    #CycleDeviation['date'] = CycleChange['date']
    #CycleDeviation = CycleDeviation[(CycleDeviation['dat'] >= CycleDeviation['date'])].drop(columns='date')
    #CycleDeviation['Linear Growth Rate'] = CycleChange['PCT']/CycleDeviation.groupby(['ticker','exchange']).size()
    #CycleDeviation = CycleDeviation.reset_index().set_index(['ticker','exchange','dat'])
    #CycleDeviation['Position'] = CycleDeviation['adj_close'].groupby(['ticker','exchange']).cumcount()+1
    #CycleDeviation['GrowthRxPosition'] = (CycleDeviation['Linear Growth Rate']*CycleDeviation['Position'])
    #CycleDeviation = CycleDeviation.sort_index(level=['ticker','exchange','dat']).reset_index('dat')
    #CycleDeviation['First Rally Price'] = CycleDeviation['adj_close'].groupby(['ticker','exchange']).head(1)
    #CycleDeviation = CycleDeviation.reset_index().set_index(['ticker','exchange','dat'])
    #CycleDeviation['Linear Growth Price'] = (CycleDeviation['GrowthRxPosition']*CycleDeviation['First Rally Price'])+CycleDeviation['First Rally Price']
    #CycleDeviation['Linear Growth Price'][CycleDeviation['Position'] == 1] = CycleDeviation['First Rally Price']
    #CycleDeviation['Deviation'] = ((CycleDeviation['Linear Growth Price']-CycleDeviation['adj_close'])/CycleDeviation['Linear Growth Price']).abs()
    #AverageDeviationSize = CycleDeviation['Deviation'].groupby(['ticker','exchange']).size()-2
    #AverageDeviationSize[AverageDeviationSize <= 0] = 0
    #AverageCycleDeviation = CycleDeviation['Deviation'].groupby(['ticker','exchange']).sum()/(AverageDeviationSize)

    Pipe = pandas.DataFrame(data={"Today's Close":TodaysClose,"Today's Adjusted Close":TodaysAdjustedClose,'Action':'Nothing','Allocation':.05,'Stop Limit Percent':-1,'Holding Period':60})
    Pipe['Potential Gain'] = PotentialGain
    Pipe['Price Position'] = PricePosition
    Pipe["Today's Volume"] = TodaysVolume
    Pipe['90 Day Average Volume'] = Volume90
    Pipe['Daily PCT Change'] = DailyPCTChange
    Pipe['Rally Duration'] = Duration
    Pipe['Rally Linear Growth Rate'] = LinerGrowthRate
    #Pipe['Rally Negative Deviation'] = AverageNegativeDeviation
    Pipe['Most Recent Trough'] = Fields[Fields['Price Position'] <= .1].groupby(['ticker','exchange']).tail(1).reset_index('dat')['dat']
    Pipe['Most Recent Peak'] = Fields[Fields['Price Position'] >= .9].groupby(['ticker','exchange']).tail(1).reset_index('dat')['dat']
    Pipe['Recent Peak or Trough'] = None
    Pipe['Recent Peak or Trough'][Pipe['Most Recent Peak'] >= Pipe['Most Recent Trough']] = 'Peak'
    Pipe['Recent Peak or Trough'][Pipe['Most Recent Trough'] >= Pipe['Most Recent Peak']] = 'Trough'
    Pipe['Largest PCT Change'] = Fields['adj_close'].pct_change().groupby(['ticker', 'exchange']).max()
    Pipe['Past Support or Ressitance'] = Fields[(Fields['Price Position'] >= 1.1) | (Fields['Price Position'] <= -.1)].groupby(['ticker','exchange']).size()/Fields['Price Position'].groupby(['ticker','exchange']).size()
    Pipe['Past Support or Ressitance'][Pipe['Past Support or Ressitance'].isna()] = 0
    Pipe['Total Size'] = Fields['Price Position'].groupby(['ticker','exchange']).size()
    Pipe['Peak Trough Cycles'] = temp.groupby(['ticker','exchange']).size()
    Pipe['Average Peak Trough Turnover'] = temp['Date diff'].groupby(['ticker','exchange']).mean()
    #Pipe['Yearly Change'] = YearlyChange
    Pipe['Peak'] = PeaksTroughs['Peak']
    Pipe['Trough'] = PeaksTroughs['Trough']
    #Pipe['Cycle Growth Rate'] = ((Pipe['Peak']-Pipe['Trough'])/Pipe['Trough'])/Pipe['Average Peak Trough Turnover']
    #Pipe['Change Since Last Cycle'] = CycleChange['PCT']
    #Pipe['Cycle Deviation'] = AverageCycleDeviation
    #Pipe['Current Cycle Duration'] = CycleDuration
    InPortfolio = Pipe.index.isin(Portfolio.index)
    Gain = ((Pipe["Today's Close"][InPortfolio] - Portfolio['Buy Price'])/Portfolio['Buy Price']) >= .01
    BuyFilter = (Pipe['Daily PCT Change'] >= .005) & (Pipe['Daily PCT Change'] <= .1) & (Pipe['Rally Duration'] >= 10) & (Pipe['Price Position'] >= .2) & (Pipe['Rally Linear Growth Rate'] >= .003) & (Pipe['Recent Peak or Trough'] == 'Trough') & (Pipe['Potential Gain'] >= .05) & (Pipe['90 Day Average Volume'] >= 20000) & (Pipe['Past Support or Ressitance'] <= .08) & (Pipe['Total Size'] >= 200) & (Pipe['Peak Trough Cycles'] >= 3)
    SellFilter = (Pipe['Price Position'] >= .9) | (Pipe["Today's Adjusted Close"] <= Pipe['Trough']) | ((Pipe['Rally Linear Growth Rate'].isna()) & Gain) | ((Pipe['Rally Linear Growth Rate'] <= .003) & Gain) | ((Pipe['Rally Duration'] < 10) & Gain) | (Pipe['Daily PCT Change'] <= (Pipe['Rally Linear Growth Rate']*-2)) & Gain & (Pipe["Today's Volume"] >= (Pipe['90 Day Average Volume']*1.75)) | (Pipe['Daily PCT Change'] <= (Pipe['Rally Linear Growth Rate']*-4)) & (Pipe["Today's Volume"] >= (Pipe['90 Day Average Volume']*1.5))
    Pipe['Stop Limit Percent'][BuyFilter] = -.5*((Pipe[BuyFilter]["Today's Adjusted Close"]-Pipe[BuyFilter]['Trough'])/Pipe[BuyFilter]["Today's Adjusted Close"])
    Pipe['Action'][BuyFilter] = 'Buy'
    Pipe['Action'][SellFilter] = 'Sell'
    Pipe.sort_values('Potential Gain',ascending=False,inplace=True)
    return Pipe
Backtest(UpTrend,End_Date='2017-01-01',Days=365,StartingOffset=365,SC=30000,BacktestTitle='RidingUpTrends30k')
