from source.Backtest_v1 import Backtest
from source.Commons import datadateslice
from source.Indicators import _EOV,_RSI
from source.Realtime_v1 import Realtime
import pandas
import datetime
from source.Commons import notify
def RSIEOV(Fields, Portfolio, datadate, firstcalc, YSignals=None):
    global RSIPrevLoss, RSIPrevGain, EOV
    if firstcalc:
        DataDays14 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=14)
        RSIData = _RSI(DataDays14, firstcalc)
        RSIGain = RSIData['diffgain']
        RSILoss = RSIData['diffloss']
        RSI = RSIData['RSI']
        EOV = _EOV(DataDays14, firstcalc)
    else:
        DataDays2 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=2)
        RSIPrevGain = YSignals['RSI Loss']
        RSIPrevLoss = YSignals['RSI Gain']
        PrevEOV = YSignals['EOV']
        RSIData = _RSI(DataDays2, firstcalc, RSIPrevGain, RSIPrevLoss)
        RSIGain = RSIData['diffgain']
        RSILoss = RSIData['diffloss']
        RSI = RSIData['RSI']
        EOV = _EOV(DataDays2, firstcalc, prevEOV=PrevEOV)
        #Todays Close
    TodaysAdjClose = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=1).reset_index('dat').drop(columns=['dat'])['adj_close']
    TodaysClose = datadateslice(Fields['close'], End_Date=datadate, Trading_Days=1).reset_index('dat').drop(columns=['dat'])['close']
    Pipe = pandas.DataFrame()
    Pipe['RSI'] = RSI
    Pipe['EOV'] = EOV
    Pipe['Action'] = 'Nothing'
    Pipe['Allocation'] = float(0)
    Pipe['Stop Limit Percent'] = float(0)
    Pipe['Holding Period'] = 14
    Pipe["Today's Close"] = TodaysClose
    Pipe["Today's Adjusted Close"] = TodaysAdjClose
    Pipe['90 Day Volume avg'] = datadateslice(Fields['volume'],Days=90,End_Date=datadate).groupby(['ticker','exchange']).mean()
    Pipe['RSI Loss'] = RSILoss
    Pipe['RSI Gain'] = RSIGain
    VolumeFilter = Pipe['90 Day Volume avg'] >= 25000
    RSIBuyFitler = Pipe['RSI'] <= 20
    EOVBuyFilter = Pipe['EOV'] <= .02
    RSISellFilter = Pipe['RSI'] >= 70
    Pipe['Action'][RSIBuyFitler & VolumeFilter & EOVBuyFilter] = 'Buy'
    Pipe['Action'][RSISellFilter] = 'Sell'
    if Portfolio is not None:
        if len(Portfolio.index) > 0:
            GainFilter = (Portfolio[Portfolio.index.isin(Pipe.index)]['Buy Price'] * 1.01).sort_index() <= Pipe[Pipe.index.isin(Portfolio.index)]["Today's Close"].sort_index()
            Pipe['Action'][(Pipe.index.isin(Portfolio.index)) & RSISellFilter & ~GainFilter] = 'Nothing'
    for x in Pipe[Pipe['Action'] == 'Buy'].index:
        if Pipe['EOV'][x] <= .005:
            StopLimit = -.01
            Allocation = .1
        elif Pipe['EOV'][x] <= .01:
            StopLimit = -.025
            Allocation = .05
        else:
            StopLimit = -.05
            Allocation = .025
        Pipe['Allocation'][x] = Allocation
        Pipe['Stop Limit Percent'][x] = StopLimit
    Pipe.sort_values('EOV',inplace=True)
    return Pipe
Backtest(RSIEOV,Days=365+90,StartingOffset=90,SC=30000,BacktestTitle='RSIEOV25kVolEOVBuyFilterADJStop&Alloc')