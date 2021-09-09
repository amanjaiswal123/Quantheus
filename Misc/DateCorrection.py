import pandas
from source.Commons import TradingDays


Data5 = pandas.read_csv('/home/hduser/Quantheus/NewPeaksTroughValues5year.csv',index_col=['ticker','exchange','dat'])
OverallData = pandas.read_csv('/home/hduser/Quantheus/Data/MarketData/BacktestData2019-03-27.csv',index_col=['ticker','exchange','dat'])
OverallData['Peak 5 Year'] = None
OverallData['Peak 5 Year'] = Data5['Peak']
OverallData['Trough 5 Year'] = None
OverallData['Trough 5 Year'] = Data5['Trough']
OverallData.to_csv('/home/hduser/Quantheus/Data/MarketData/NewBacktestData2019-02-22.csv')